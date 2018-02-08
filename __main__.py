from bitcoinrpc.authproxy import AuthServiceProxy
from config import RPCuser, RPCpassword, RPCport
from pymongo import MongoClient
from sys import exit

from time import sleep

def main():
	client = MongoClient()
	db = client['blockchain']
	blocksDB = db['blocks']
	txDB = db['tx']

	rpc = AuthServiceProxy("http://%s:%s@127.0.0.1:%i"%(RPCuser, RPCpassword, RPCport))

	blockCount = rpc.getblockcount()
	lastBlock = blocksDB.count()

	print("Last block in DB: %i" % (lastBlock - 1))
	print("Last block in network: %i" % blockCount)
	print("Tx captured: %i" % txDB.count())

	if blockCount > lastBlock - 1:
		for i in range(lastBlock, blockCount + 1):
			block = rpc.getblock(rpc.getblockhash(i))
			block['difficulty'] = float(block['difficulty'])
			if 'nextblockhash' in block:
				del block['nextblockhash']

			print(block['height'], (block['hash']), blocksDB.insert_one(block))

			if block['height'] > 0:
				for j in block['tx']:
					rawTx = rpc.getrawtransaction(j)
					tx = rpc.decoderawtransaction(rawTx)
					for k in tx['vout']:
						k['value'] = '{0:.8f}'.format(k['value'])
					print(tx['txid'], txDB.insert_one(tx))


if __name__ == '__main__':
	print("UKUKU BLOCKCHAIN EXTRACTOR V 0.1")

	while True:
		try:
			main()
			print("sleeping...")
			sleep(60)

		except (KeyboardInterrupt, SystemExit):
			print("vendisuone ~")
			exit()