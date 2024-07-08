def GetConfig():
    config = {}
    config['url'] = 'https://jmcauley.ucsd.edu/data/amazon_v2/categoryFilesSmall/Electronics_5.json.gz'

    config['filename_gz'] = 'Electronics_5.json.gz'
    config['filename_unzipped'] = 'Electronics_5.json'
    config['result_file'] = 'final_result.csv'
    config['log_directory'] = 'logs'
    config['db_filename'] = 'amazon_reviews.db'
    config['from_email'] = 'kaushik.raj8@gmail.com'
    config['to_email'] = 'rishabhbhardwaj316@gmail.com'
    config['smtp_server'] = 'smtp-relay.brevo.com'
    config['smtp_port'] = 587
    config['smtp_username'] = '781108001@smtp-brevo.com'
    config['smtp_password'] = 'XMDy90xg1VTjm5GY'
    config['subject'] = 'Daily Review Report'
    config['chunksize'] = 10 ** 6
    return config
    