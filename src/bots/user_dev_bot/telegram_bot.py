from src.bots.common.hltv_user_bot import HLTVUserBot
from src.bots.config import load_config

if __name__ == "__main__":
    config = load_config('user_dev')
    bot = HLTVUserBot(config['token'], config['hltv_db_path'], config['log_file'], config_name='user_dev')
    bot.run()