
import click

from chair.cli import processor

@click.command()
@processor
@click.pass_context
def cli(ctx, sink):

    """
    This command consumes and produces a python dict for each bitcoin block.
    The input comes from the blockchair.com data.
    This function adds a number of keys to each.

    Using the as-yet unreleased chair framework, it can be run like this:

    chair while_true csv_dictreader --dialect excel-tab --no-unicode bitcoin_blocks.tsv rate -v btc_ltv tsv -d , print > computed.csv
    """

    import math
    from datetime import datetime

    prev = dict(TOTAL_SATS=0, TOTAL_HASHES=0, id=-1)

    while True:
        try:
            block = yield
        except GeneratorExit:
            break

        assert int(block['id']) == int(prev['id']) + 1 # data must be sorted

        if block['generation_usd'] == '0':
            # the data we're using from https://gz.blockchair.com/bitcoin/blocks/
            # recorded zero minted coins in one block:
            assert block['id'] == '501726'
            block['generation_usd'] = prev['generation_usd']
            block['generation'] = prev['generation']

        # the data also has not-quite-right values for generated sometimes (a
        # few satoshis short of the actual reward)... this fix will last
        # through the next halvening, i hope:
        block['GENERATION'] = int(round(float(block['generation']) / 10**8 / 8 * 100) / 100 * 8) * 10**8
        block['GENERATION'] = sorted([(50*10**8)/2**n for n in range(33)], key=lambda k: abs(float(block['generation'])-k))[0]
        
        block['date'] = block['median_time'].split(' ')[0]
        block['month'] = block['median_time'][:7]
        block['year'] = block['median_time'][:4]
        block['DATETIME'] = datetime.strptime(block['median_time'], '%Y-%m-%d %H:%M:%S')
        
        generation_usd = float(block['generation_usd'])
        hashes_per_block = float(block['difficulty']) * 2**48 / 0xffff # https://en.bitcoin.it/wiki/Difficulty

        block['USD_PER_BLOCK'] = float(block['generation_usd'])
        block['BTC_PER_BLOCK'] = float(block['generation'])/10**8
        block['TOTAL_SATS'] = prev['TOTAL_SATS'] + int(block['generation'])
        block['TOTAL_BTC'] = (block['TOTAL_SATS'] / 10**8)
        block['TOTAL_HASHES'] = prev['TOTAL_HASHES'] + hashes_per_block
        block['TOTAL_EHASHES'] = block['TOTAL_HASHES'] / (10**18)
        block['HASHES_PER_BLOCK'] = hashes_per_block
        block['HASHRATE'] = hashes_per_block / 60 / 10
        block['HASHRATE_LOG10'] = math.log10(block['HASHRATE'])
        block['USD_PER_BTC'] =  generation_usd / (float(block['generation'])/10**8)
        block['MARKET_CAP'] = block['TOTAL_BTC'] * block['USD_PER_BTC']
        block['MARKET_CAP_M'] = block['MARKET_CAP'] / 10**6
        block['USD_PER_HASH']  = generation_usd / hashes_per_block
        block['USD_PER_THASH'] = generation_usd / (hashes_per_block / (10**12))
        block['USD_PER_PHASH'] = generation_usd / (hashes_per_block / (10**15))
        block['USD_PER_EHASH'] = generation_usd / (hashes_per_block / (10**18))
        block['HASHES_PER_USD']  = 1 / block['USD_PER_HASH']
        block['THASHES_PER_USD'] = 1 / block['USD_PER_THASH']
        block['PHASHES_PER_USD'] = 1 / block['USD_PER_PHASH']
        block['EHASHES_PER_USD'] = 1 / block['USD_PER_EHASH']
        block['LTV_USD_PER_HASH']  = block['MARKET_CAP'] / block['TOTAL_HASHES']
        block['LTV_USD_PER_THASH'] = block['MARKET_CAP'] / (block['TOTAL_HASHES'] / (10**12))
        block['LTV_USD_PER_PHASH'] = block['MARKET_CAP'] / (block['TOTAL_HASHES'] / (10**15))
        block['LTV_USD_PER_EHASH'] = block['MARKET_CAP'] / (block['TOTAL_HASHES'] / (10**18))
        block['LTV_HASHES_PER_USD']  = 1 / block['LTV_USD_PER_HASH']
        block['LTV_THASHES_PER_USD'] = 1 / block['LTV_USD_PER_THASH']
        block['LTV_PHASHES_PER_USD'] = 1 / block['LTV_USD_PER_PHASH']
        block['LTV_EHASHES_PER_USD'] = 1 / block['LTV_USD_PER_EHASH']
        block['THE_RATIO'] = block['LTV_HASHES_PER_USD'] / block['HASHES_PER_USD']
        block['THE_RATIO_I'] = 1/ block['THE_RATIO']

        block['74TH_DAILY_REVENUE'] = block['USD_PER_THASH'] * 74 * 60 * 60 * 24
        block['LTV_74TH_DAILY_REVENUE'] = block['LTV_USD_PER_THASH'] * 74 * 60 * 60 * 24
        block['DAYS_TO_MINE_50USD_AT_20_MHASH'] = (block['LTV_HASHES_PER_USD'] * 50.0) / (20*10**6) / 60 / 60 / 24
        block['WEEKS_TO_MINE_A_CENT_AT_20_MHASH'] = (block['LTV_HASHES_PER_USD'] / 100) / (20*10**6) / 60 / 60 / 24 / 7
        block['MINUTES_TO_MINE_50_CENTS_AT_7_MHASH'] = (block['LTV_HASHES_PER_USD'] / 100 * 50) / (7*10**6) / 60

#        block['COINBASE_DATA'] = codecs.decode(block['coinbase_data_hex'], 'hex')
#        block['COINBASE_DATA_ASCII'] = "".join(b for b in block['COINBASE_DATA'] if 0x20 <= ord(b) <= 0x7e)
        
        prev = block.copy()
        sink.send(block)

