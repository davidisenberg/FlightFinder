
import requests
import json
from services.core.directsservice import DirectService

def process():

    try:
        completed = []
        queue = ['LHR']

        while len(queue) > 0:
            chosen = queue[0]
            queue.remove(chosen)
            if chosen in completed:
                continue

            completed.append(chosen);
            directs = DirectService().get_directs(chosen)
            for index, data in directs.iterrows():
                direct = data["FlyTo"]
                if direct in completed or direct in queue :
                    continue
                queue.append(direct)
    except Exception as e:
        print(e)

process()