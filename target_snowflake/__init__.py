import singer
from singer import utils
from target_postgres import target_tools
from target_redshift.s3 import S3

from target_snowflake.connection import connect
from target_snowflake.snowflake import SnowflakeTarget
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

LOGGER = singer.get_logger()

# Modified to have either password or private key, not both required
REQUIRED_CONFIG_KEYS = [
    'snowflake_account',
    'snowflake_warehouse',
    'snowflake_database',
    'snowflake_username',
]

def validate_config(config):
    """Validate that either password or private key is provided"""
    if not config.get('snowflake_password') and not config.get('snowflake_private_key'):
        raise Exception("Either 'snowflake_password' or 'snowflake_private_key' must be provided")
    if config.get('snowflake_password') and config.get('snowflake_private_key'):
        LOGGER.warning("Both password and private key provided. Using private key authentication.")

def get_private_key(config):
    """Get private key for authentication"""
    try:
        # Convert private key to bytes if it's a string
        private_key = config.get('snowflake_private_key')
        if isinstance(private_key, str):
            private_key = private_key.encode('utf-8')

        p_key = serialization.load_pem_private_key(
            private_key,
            password=None,
            backend=default_backend(),
        )

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    except Exception as e:
        raise Exception(f"Error processing private key: {str(e)}") from e

def main(config, input_stream=None):
    # Validate authentication configuration
    validate_config(config)

    # Prepare authentication parameters
    connect_params = {
        'user': config.get('snowflake_username'),
        'role': config.get('snowflake_role'),
        'authenticator': config.get('snowflake_authenticator', 'snowflake'),
        'account': config.get('snowflake_account'),
        'warehouse': config.get('snowflake_warehouse'),
        'database': config.get('snowflake_database'),
        'schema': config.get('snowflake_schema', 'PUBLIC'),
        'autocommit': False
    }

    # Add authentication method based on provided credentials
    if config.get('snowflake_private_key'):
        connect_params['private_key'] = get_private_key(config)
    else:
        connect_params['password'] = config.get('snowflake_password')

    with connect(**connect_params) as connection:
        s3_config = config.get('target_s3')

        s3 = None
        if s3_config:
            s3 = S3(s3_config.get('aws_access_key_id'),
                    s3_config.get('aws_secret_access_key'),
                    s3_config.get('bucket'),
                    s3_config.get('key_prefix'))

        target = SnowflakeTarget(
            connection,
            s3=s3,
            logging_level=config.get('logging_level'),
            persist_empty_tables=config.get('persist_empty_tables'),
            external_stage=config.get('external_stage')
        )

        if input_stream:
            target_tools.stream_to_target(input_stream, target, config=config)
        else:
            target_tools.main(target)

def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    main(args.config)
