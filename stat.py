from pyspark import SparkContext, SparkConf

def doMap(str):
	index = str.find('chinaDaas')
	if index == -1:
		return ''
	else:
		dt = str[:index]
		if dt != '':
			findex = dt.find('2017-07')
			if findex == -1:
				return ''
			dt = dt[findex:]
			endindex = str.find('result:')
			if endindex == -1:
				url = str[index + 10:]
			else:
				url = str[index + 10: endindex]
			return (dt, url)
		return ''

sc = SparkContext('local', 'test')
distFile = sc.textFile("hdfs://172.19.6.50:8022/portal/chinaDaasLog")
rdd = distFile.map(doMap).filter(lambda x: x != '').collect()
print(rdd)
print(len(rdd))
