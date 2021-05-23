import argparse
import asyncio
import sys
import resource
from aiohttp import ClientSession
from aiohttp import TCPConnector
from functools import partial
import csv



# This function  used for
# Read the CSv
def getstuff(filename):
    with open(filename, "r") as csvfile:
        datareader = csv.reader(csvfile)
        yield next(datareader)  # yield the header row
        for row in datareader:
                yield row


def get_mem():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1000000

async def run(session, concurrent_limit):
    base_url = "http://localhost:8888/{}"
    tasks = []
    responses = []
    sem = asyncio.Semaphore(concurrent_limit)

    async def fetch(url, i):
        try:
            async with session.get(url) as response:
                response = await response.read()
                sem.release()
                responses.append(response)
                if i % 100 == 0:
                    print("n:{:10d} tasks:{:10d} {:.1f}MB".format(i, len(tasks), get_mem()))
                return response
        except Exception as e:
            print("Err :", url, e)

    for i, v in enumerate(getstuff("./url_cam.csv")):
        if i < 500:
            if i == 0 :
                continue
            await sem.acquire()
            # url = "https://{}".format(v[0]) # read the url 
            url = v[0]
            task = asyncio.ensure_future(fetch(url, i))
            task.add_done_callback(tasks.remove)
            tasks.append(task)
        else:
            break

    await asyncio.wait(tasks)
    print("total_responses: {}".format(len(responses)))
    # print(responses)
    return responses

async def main(concurrent_limit):
    async with ClientSession(trust_env=True,connector=TCPConnector(ssl=False)) as session:
        responses = await asyncio.ensure_future(run(session, concurrent_limit))
        return

if __name__ == '__main__':
    """
    run: python client.py -n 1000 
    """
    print("Welcome to Mocks Million web scrapper")
    parser = argparse.ArgumentParser()
    parser.add_argument('-c')
    args = parser.parse_args()
    concurrent_limit = int(args.c) if args.c else 1000
    print("concurrent_limit: {}".format(concurrent_limit))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(concurrent_limit))
    loop.close()
    