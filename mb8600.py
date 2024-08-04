import aiochclient
import aiohttp
import asyncio
import colorlog
import datetime
import hashlib
import hmac
import json
import logging
import math
import os
import re
import signal
import sys
import time

from time import perf_counter

log = logging.getLogger('mb8600')

UPTIME_REGEX = re.compile(r'(?:(\d+)\s*days\s*)?(?:(\d{2})h:)?(?:(\d{2})m:)?(?:(\d{2})s)?')

class MB8600:
    def __init__(self, loop):
        # Setup logging
        self._setup_logging()
        # Load environment variables
        self._load_env_vars()

        # Event loop
        self.loop = loop

        # Queue of data waiting to be inserted into ClickHouse
        self.clickhouse_queue = asyncio.Queue(maxsize=self.clickhouse_queue_limit)

        # Data needed for HNAP authentication
        # Generated during the login process
        self.modem_hnap_session = {
            'challenge': None,
            'uid': None,
            'public_key': None,
            'private_key': None,
            'login_password': None
        }

        # Event used to stop the loop
        self.stop_event = asyncio.Event()

    def _setup_logging(self):
        """
            Sets up logging colors and formatting
        """
        # Create a new handler with colors and formatting
        shandler = logging.StreamHandler(stream=sys.stdout)
        shandler.setFormatter(colorlog.LevelFormatter(
            fmt={
                'DEBUG': '{log_color}{asctime} [{levelname}] {message}',
                'INFO': '{log_color}{asctime} [{levelname}] {message}',
                'WARNING': '{log_color}{asctime} [{levelname}] {message}',
                'ERROR': '{log_color}{asctime} [{levelname}] {message}',
                'CRITICAL': '{log_color}{asctime} [{levelname}] {message}',
            },
            log_colors={
                'DEBUG': 'blue',
                'INFO': 'white',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bg_red',
            },
            style='{',
            datefmt='%d/%m/%Y %H:%M:%S'
        ))
        # Add the new handler
        logging.getLogger('mb8600').addHandler(shandler)
        log.debug('Finished setting up logging')

    def _load_env_vars(self):
        """
            Loads environment variables and sets defaults
        """
        # Modem name (str, default: "MB8600")
        self.modem_name = os.environ.get('MODEM_NAME', 'MB8600')

        # Handle required environment variables
        try:
            # Modem URL (str)
            self.modem_url = os.environ['MODEM_URL']
            # Modem username (str)
            self.modem_username = os.environ['MODEM_USERNAME']
            # Modem password (str)
            self.modem_password = os.environ['MODEM_PASSWORD']
            # ClickHouse URL (str)
            self.clickhouse_url = os.environ['CLICKHOUSE_URL']
            # ClickHouse username (str)
            self.clickhouse_username = os.environ['CLICKHOUSE_USERNAME']
            # ClickHouse password (str)
            self.clickhouse_password = os.environ['CLICKHOUSE_PASSWORD']
            # ClickHouse database (str)
            self.clickhouse_database = os.environ['CLICKHOUSE_DATABASE']
        except KeyError as e:
            log.critical(f'Missing environment variable: {e}')
            exit(1)

        # ClickHouse table name (str, default: "docsis")
        self.clickhouse_table = os.environ.get('CLICKHOUSE_TABLE', 'docsis')

        # Scrape delay (int, default: 10)
        try:
            self.scrape_delay = int(os.environ.get('SCRAPE_DELAY', 10))
            # Make sure the scrape delay is at least 1 second
            if self.scrape_delay < 1:
                raise ValueError
        except ValueError:
            log.critical('Invalid SCRAPE_DELAY, must be a valid number >= 1')
            exit(1)

        # ClickHouse queue limit (int, default: 1000)
        try:
            self.clickhouse_queue_limit = int(os.environ.get('CLICKHOUSE_QUEUE_LIMIT', 1000))
            # Make sure the queue limit is at least 25
            if self.clickhouse_queue_limit < 25:
                raise ValueError
        except ValueError:
            log.critical('Invalid CLICKHOUSE_QUEUE_LIMIT, must be a valid number >= 25')
            exit(1)

        try:
            log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
            if log_level not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
                raise ValueError
        except ValueError:
            log.critical('Invalid LOG_LEVEL, must be a valid log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
            exit(1)

        # Set the log level
        log.setLevel({'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL}[log_level])

    def generate_private_key(self, public_key: str, challenge: str) -> str:
        """
            Generates the private key used for HNAP authentication
        """
        # Generate the private key
        private_key = hmac.new(
            f'{public_key}{self.modem_password}'.encode(),
            challenge.encode(),
            hashlib.md5
        ).hexdigest().upper()

        log.debug(f'Generated private key: {private_key}')

        return private_key
    
    def generate_login_password(self, private_key: str, challenge: str) -> str:
        """
            Generates the login password used for HNAP authentication
        """
        # Generate the login password
        login_password = hmac.new(
            private_key.encode(),
            challenge.encode(),
            hashlib.md5
        ).hexdigest().upper()

        log.debug(f'Generated login password: {login_password}')

        return login_password

    def generate_hnap_auth(self, soap_action: str, private_key: str="withoutloginkey") -> str:
        """
            Generates the HNAP AUTH header
        """
        # Get the current time in milliseconds
        current_time = int(time.time() * 1000)
        current_time = math.floor(current_time) % 2000000000000

        soap_action_uri = f'http://purenetworks.com/HNAP1/{soap_action}'

        # Generate the MD5 HMAC and uppercase it
        auth = hmac.new(
            private_key.encode(),
            f'{current_time}{soap_action_uri}'.encode(),
            hashlib.md5
        ).hexdigest().upper()
        # Combine the auth string with the time (again, for some reason)
        auth = f'{auth} {current_time}'
        log.debug(f'Generated Hnap_auth: {auth}')
        return auth
    
    async def login(self):
        hnap_url = f'{self.modem_url}/HNAP1/'
        soap_action = 'Login'
        soap_action_uri = f'http://purenetworks.com/HNAP1/{soap_action}'

        initial_hnap_auth = self.generate_hnap_auth(soap_action)

        # Fetch the initial LoginResponse (Challenge, Cookie, PublicKey)
        async with self.session.post(
            hnap_url,
            headers={
                'Hnap_auth': initial_hnap_auth,
                'Soapaction': soap_action_uri
            },
            json={
                'Login': {
                    'Action': 'request',
                    'Username': self.modem_username,
                    'LoginPassword': '',
                    'Captcha': '',
                    'PrivateLogin': 'LoginPassword'
                }
            }
        ) as resp:
            log.debug(f'Got login request response HTTP {resp.status} {resp.reason}: {await resp.text()}')
            # Decode the respnse (JSON returned with html content type, why???)
            login_response = await resp.json(content_type='text/html')

        challenge = login_response['LoginResponse']['Challenge']
        cookie = login_response['LoginResponse']['Cookie']
        public_key = login_response['LoginResponse']['PublicKey']
        # Generate the private key
        private_key = self.generate_private_key(public_key, challenge)
        # Generate the login password
        login_password = self.generate_login_password(private_key, challenge)
        # Generate the HNAP_AUTH header
        hnap_auth = self.generate_hnap_auth(soap_action, private_key)

        # Store the session data
        self.modem_hnap_session['challenge'] = challenge
        self.modem_hnap_session['uid'] = cookie
        self.modem_hnap_session['public_key'] = public_key
        self.modem_hnap_session['private_key'] = private_key
        self.modem_hnap_session['login_password'] = login_password
        self.modem_hnap_session['hnap_auth'] = hnap_auth

        # Login with the generated login password and HNAP variables
        async with self.session.post(
            hnap_url,
            cookies={'uid': cookie, 'PrivateKey': private_key},
            headers={
                'Hnap_auth': hnap_auth,
                'Soapaction': soap_action_uri
            },
            json={
                'Login': {
                    'Action': 'login',
                    'Username': self.modem_username,
                    'LoginPassword': login_password,
                    'Captcha': '',
                    'PrivateLogin': 'LoginPassword'
                    }
            }
        ) as resp:
            # Decode the respnse (JSON returned with html content type, why???)
            login_response = await resp.json(content_type='text/html')
            log.debug(f'Got login response HTTP {resp.status} {resp.reason}: {login_response}')
            if login_response['LoginResponse']['LoginResult'] != 'OK':
                raise Exception('Invalid username or password')
            else:
                log.info('Logged in')

    async def run(self):
        # Create a ClientSession that doesn't verify SSL certificates
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        )
        self.clickhouse = aiochclient.ChClient(
            self.session,
            url=self.clickhouse_url,
            user=self.clickhouse_username,
            password=self.clickhouse_password,
            database=self.clickhouse_database,
            json=json
        )
        # Cookies used for auth
        self.cookies = {}

        # Start the ClickHouse insert task
        insert_task = self.loop.create_task(self.insert_into_clickhouse())

        # Start the exporter in a background task
        export_task = self.loop.create_task(self.export())

        # Wait for the stop event
        await self.stop_event.wait()

        # Close the aiohttp session
        await self.session.close()

        # Cancel the exporter task
        export_task.cancel()
        # Cancel the ClickHouse insert task
        insert_task.cancel()

    async def insert_into_clickhouse(self):
        """
            Insert queue'd data into ClickHouse
        """
        while True:
            try:
                # Get the data from the queue
                data = await self.clickhouse_queue.get()
                log.debug(f'Inserting data into ClickHouse: {data}')
                # Insert the data into ClickHouse
                await self.clickhouse.execute(
                    data[0],
                    *data[1]
                )
            except Exception as e:
                log.error(f'Failed to insert data into ClickHouse: {e}')
                # Wait before we retry inserting
                await asyncio.sleep(5)

    async def export(self):
        try:
            # Generate an initial session
            await self.login()
        except Exception as e:
            log.critical(f'Login failed: {e}')
            self.stop_event.set()
            return

        modem_hnap_url = f'{self.modem_url}/HNAP1/'
        modem_cookies = {'uid': self.modem_hnap_session['uid'], 'PrivateKey': self.modem_hnap_session['private_key']}
        modem_headers = {
            'Hnap_auth': self.modem_hnap_session['hnap_auth'],
            'Soapaction': 'http://purenetworks.com/HNAP1/GetMultipleHNAPs',
        }

        while True:
            try:
                start = perf_counter()

                # Update the Hnap_auth header since it's timestamp based
                modem_headers['Hnap_auth'] = self.generate_hnap_auth('GetMultipleHNAPs', self.modem_hnap_session['private_key'])
                # Login with the generated login password and HNAP variables
                async with self.session.post(
                    modem_hnap_url,
                    cookies=modem_cookies,
                    headers=modem_headers,
                    json={
                        'GetMultipleHNAPs': {
                            'GetMotoStatusStartupSequence': '',         # Modem status
                            'GetMotoStatusConnectionInfo': '',          # System uptime
                            'GetMotoStatusDownstreamChannelInfo': '',   # Downstream channels
                            'GetMotoStatusUpstreamChannelInfo': '',     # Upstream channels
                            'GetMotoStatusSoftware': '',                # Modem management/software status
                        }
                    }
                ) as resp:
                    # Decode the respnse (JSON returned with html content type, why???)
                    modem_response = await resp.json(content_type='text/html')
                    log.debug(f'Got modem status response HTTP {resp.status} {resp.reason}: {modem_response}')
                    # Check if the response was successful
                    if modem_response['GetMultipleHNAPsResponse']['GetMultipleHNAPsResult'] != 'OK':
                        # Session most likely expired, try to login again
                        log.warning('Session expired, trying to login again')
                        await self.login()
                        # Wait before we retry scraping
                        await asyncio.sleep(self.scrape_delay)
                        continue

                scraping_latency = perf_counter() - start
                log.info(f'Modem status scraping complete, took {round(scraping_latency, 2)}s')

                # Get the current UTC timestamp
                timestamp = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()

                # Downstream channels
                downstream_channels = []
                for channel in modem_response['GetMultipleHNAPsResponse']['GetMotoStatusDownstreamChannelInfoResponse']['MotoConnDownstreamChannel'].split('|+|'):
                    _, _, modulation, channel_id, frequency, power, snr, correcteds, uncorrecteds, _ = channel.split('^')
                    if (modulation == 'OFDM PLC'):
                        # Check if the OFDM SNR bug is present
                        if (snr := float(snr)) < 20.0:
                            # We need to correct the SNR value by about 2.5x
                            snr *= 2.5
                    
                    downstream_channels.append([(
                        int(channel_id),                # Channel ID
                        float(frequency) * 1000000,     # Frequency (converted to MHz)
                        modulation,                     # Modulation
                        float(power),                   # Power (dBmV)
                        float(snr),                     # SNR (dB)
                        int(correcteds),                # Correcteds
                        int(uncorrecteds),              # Uncorrecteds
                    )])

                # Upstream channels
                upstream_channels = []
                for channel in modem_response['GetMultipleHNAPsResponse']['GetMotoStatusUpstreamChannelInfoResponse']['MotoConnUpstreamChannel'].split('|+|'):
                    _, _, modulation, channel_id, width, frequency, power, _ = channel.split('^')
                    upstream_channels.append([(
                        int(channel_id),                # Channel ID
                        float(frequency) * 1000000,     # Frequency (converted to MHz)
                        modulation,                     # Modulation
                        float(power),                   # Power (dBmV)
                        float(width) * 1000,            # Width (converted to MHz)
                    )])

                # Parse device uptime
                uptime = 0
                uptime_groups = UPTIME_REGEX.search(modem_response['GetMultipleHNAPsResponse']['GetMotoStatusConnectionInfoResponse']['MotoConnSystemUpTime']).groups()
                # Days
                uptime += int(uptime_groups[0]) * 86400
                # Hours
                uptime += int(uptime_groups[1]) * 3600
                # Minutes
                uptime += int(uptime_groups[2]) * 60
                # Seconds
                uptime += int(uptime_groups[3])

                ok = [(
                        self.modem_name,                                                                                                        # Modem name
                        modem_response['GetMultipleHNAPsResponse']['GetMotoStatusStartupSequenceResponse']['MotoConnConfigurationFileComment'], # Modem DOCSIS configuration filename
                        uptime,                                                                                                                 # Modem uptime
                        modem_response['GetMultipleHNAPsResponse']['GetMotoStatusSoftwareResponse']['StatusSoftwareSfVer'],                     # Modem software version
                        'MB8600',                                                                                                               # Modem model
                        downstream_channels,                                                                                                    # Downstream channels
                        upstream_channels,                                                                                                      # Upstream channels
                        scraping_latency,                                                                                                       # Scraping latency
                        timestamp                                                                                                               # Data timestamp
                    )]

                # Insert data into ClickHouse
                await self.clickhouse_queue.put((
                    f"INSERT INTO {self.clickhouse_table} (modem_name, modem_config_filename, modem_uptime, modem_version, modem_model, downstream_channels, upstream_channels, scrape_latency, timestamp) VALUES",
                    [(
                        self.modem_name,                                                                                                        # Modem name
                        modem_response['GetMultipleHNAPsResponse']['GetMotoStatusStartupSequenceResponse']['MotoConnConfigurationFileComment'], # Modem DOCSIS configuration filename
                        uptime,                                                                                                                 # Modem uptime
                        modem_response['GetMultipleHNAPsResponse']['GetMotoStatusSoftwareResponse']['StatusSoftwareSfVer'],                     # Modem software version
                        'MB8600',                                                                                                               # Modem model
                        downstream_channels,                                                                                                    # Downstream channels
                        upstream_channels,                                                                                                      # Upstream channels
                        scraping_latency,                                                                                                       # Scraping latency
                        timestamp                                                                                                               # Data timestamp
                    )]
                ))
            except Exception as e:
                log.error(f'Failed to update modem status: {e}')
                await asyncio.sleep(self.scrape_delay)
            finally:
                await asyncio.sleep(self.scrape_delay)

loop = asyncio.new_event_loop()
exporter = MB8600(loop)

def sigterm_handler(_signo, _stack_frame):
    """
        Handle SIGTERM
    """
    # Set the event to stop the loop
    exporter.stop_event.set()
# Register the SIGTERM handler
signal.signal(signal.SIGTERM, sigterm_handler)

loop.run_until_complete(exporter.run())
