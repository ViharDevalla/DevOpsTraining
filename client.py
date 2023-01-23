import pandas as pd
import json
import requests
import argparse
import time


import asyncio
import aiohttp
import concurrent.futures

def get_status(ip):
    url = f"http://localhost/{ip}"
    resp = requests.get(url=url)
    ipData = json.loads(resp.text.replace("%",""))
    ipData["ip"] = ip
    ipData["healthy"] = "Healthy" if int(ipData['cpu'])<80 and int(ipData['memory'])<80  else 'Unhealthy'
    return ipData


def multi_thread(ips):
    with concurrent.futures.ThreadPoolExecutor() as executor:

        futures = []

        for ip in ips:
            futures.append(executor.submit(get_status, ip=ip))
        arr=[]
        for future in concurrent.futures.as_completed(futures):
            arr.append(future.result())
    return setdf(arr)




async def get_ip(session, url):
    async with session.get(url) as resp:
        ipData = await resp.json()
        return ipData



async def by_aiohttp_concurrency(ips):
    # use aiohttp

    async with aiohttp.ClientSession() as session:
        tasks = []
        for ip in ips:
            print("ip",ip)
            url = f"http://localhost/{ip}"
            tasks.append(asyncio.ensure_future(get_ip(session, url)))

        original_ips = await asyncio.gather(*tasks)

        for ip in original_ips:
            print(ip)
        session.close()


def makeCache():
    df = pd.DataFrame(columns=['ip','cpu','memory','service','healthy'])
    df.to_csv('data_cache.csv',index=False)

def getCache():
    df = pd.read_csv('data_cache.csv')
    return df

def setCache(df):
    df.to_csv('data_cache.csv',index=False)


def getAllServers():
    df = pd.DataFrame([],columns=['ip','cpu','memory','service','healthy'])
    url = "http://localhost/servers"
    res = requests.get(url)
    allServerData = res.text.strip('][').replace('"','').split(', ')
    df['ip'] = allServerData
    setCache(df)
    return allServerData



def setdf(data):
    df = pd.DataFrame(data,columns=['ip','cpu','memory','service','healthy'])
    df.to_csv('data_cache.csv',index=False)
    return df

def getOneServer(df,ip):
    try:
        url = f"http://localhost/{ip}"
        res = requests.get(url)
    except Exception as e:
        print("Network Error - Check if IP and port is correct")
        print(e)

    try:
        oneServer = json.loads(res.text)
        oneServer['ip'] = ip

        df.loc[df['ip'] == ip,'ip'] = oneServer['ip']
        df.loc[df['ip'] == ip,'cpu'] = int(oneServer['cpu'][:-1])
        df.loc[df['ip'] == ip,'memory'] = int(oneServer['memory'][:-1])
        df.loc[df['ip'] == ip,'service'] = oneServer['service']
        df.loc[df['ip'] == ip,'healthy'] =  "Healthy" if int(oneServer['cpu'][:-1])<80 and int(oneServer['memory'][:-1])<80 else "Unhealthy"
    except Exception as e:
        print("Error in parsing server details")
    setCache(df)
    return oneServer

#makeCache()
def updateAllServers(df,ips):
    for ip in ips:
        getOneServer(df,ip)
    return True



def printServices():
    df = getCache()
    return df


def AverageForService(df,service):
    servicedf = df.loc[df['service']==service]
    return servicedf['cpu'].mean(),servicedf['memory'].mean()

def UnhealthyAlert():
    df = getCache()
    healthyCount = df[df['healthy']=="Unhealthy"].groupby('service').count()
    print(healthyCount)
    return healthyCount[healthyCount['ip']<5]["ip"]

def trackService(service,sec):
    ips = df.loc[df['service']==service]["ip"].to_list()

    try:
        while(True):
            data = multi_thread(ips)
            print(data)
            print("\n")
            time.sleep(sec)
    except KeyboardInterrupt:
        print("Tracking Stopped")



try:


    if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Health Check of Services and Machines')
        parser.add_argument("-a","--all", help="Get details on all the servers",action="store_true")
        parser.add_argument("-af","--check", help="Faster - Get details on all the servers",action="store_true")
        parser.add_argument("-uh","--unhealthy", help="Find all services which might fail",action="store_true")
        parser.add_argument("-s","--service", help="Service based details of instances",type=str,metavar='SERVICE')
        parser.add_argument("-i","--ip", help="Get details on based on ip",type=str,metavar='IP')
        parser.add_argument("-ts","--trackservice", help="Track a particular service periodically over a time interval",nargs=2,metavar=("SERVICE", "TIME"))
        args = parser.parse_args()

        print(args)
        df = getCache()

        if(args.all):
            start_time = time.time()
            fullData = getAllServers()
            updateAllServers(df,fullData)
            finalData = getCache()
            print(finalData.to_string())
            print("Time taken",time.time() - start_time)

        if(args.ip):
            start_time = time.time()
            ipData = getOneServer(df,args.ip)
            print(ipData)
            print("Time taken",time.time() - start_time)


        if(args.check):
            start_time = time.time()
            fullData = getAllServers()
            df = multi_thread(fullData)
            print(df.to_string())
            print("Time taken",time.time() - start_time)


        if(args.service):
            start_time = time.time()
            cpu,memory = AverageForService(df,args.service)
            print("Service",args.service)
            print("Average CPU :",cpu)
            print("Average Memory :",memory)
            print("Time taken",time.time() - start_time)


        if(args.unhealthy):
            start_time = time.time()
            data = UnhealthyAlert()
            print(data)
            print("Time taken",time.time() - start_time)


        if(args.trackservice):
            start_time = time.time()
            trackService(args.trackservice[0],int(args.trackservice[1]))
            print("Time taken",time.time() - start_time)

except Exception as e:
    print("Error has Occured")
    print(e)

