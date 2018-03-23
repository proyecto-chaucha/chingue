from config import RPCuser, RPCpassword, RPCport
from bitcoinrpc.authproxy import AuthServiceProxy
from pymongo import MongoClient, DESCENDING
from time import sleep
from sys import exit

def main():
	# MongoDB
	client = MongoClient()
	db = client['blockchain']

	# Collections
	blocksDB = db['blocks']
	txDB = db['tx']
	diffDB = db['diff']

	# JSON-RPC
	rpc = AuthServiceProxy("http://%s:%s@127.0.0.1:%i"%(RPCuser, RPCpassword, RPCport))

	# Get last values
	blockCount = rpc.getblockcount()
	lastBlock = blocksDB.count()

	# Debug
	print("- Last block in DB: %i" % (lastBlock - 1))
	print("- Last block in network: %i" % blockCount)
	print("- Tx captured: %i" % txDB.count())

	# Capture
	if blockCount > lastBlock - 1:
		sleep(0.1)
		for i in range(lastBlock, blockCount + 1):
			print("- Block #%i" % i)

			# GetBlock
			block = rpc.getblock(rpc.getblockhash(i))

			if block['height'] > 0:
				# Remove nextblockhash
				if 'nextblockhash' in block:
					del block['nextblockhash']

				# GetTX
				for j in block['tx']:
					rawTx = rpc.getrawtransaction(j)
					tx = rpc.decoderawtransaction(rawTx)
					
					# Format value
					for k in tx['vout']:
						k['value'] = '{0:.8f}'.format(float(k['value']))

					txDB.insert_one(tx) # Insert

				# format difficulty
				block['difficulty'] = float(block['difficulty'])
				blocksDB.insert_one(block) # Insert

	print("- Done.")

if __name__ == '__main__':
	print("UKUKU BLOCKCHAIN EXTRACTOR V 0.1")
	main()
