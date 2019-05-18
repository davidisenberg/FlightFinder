
import requests
import json

def process():

    completed = []
    queue = ['LHR']

    while len(queue) > 0:
        chosen = queue[0]
        queue.remove(chosen)
        if chosen in completed:
            continue

        completed.append(chosen);
        directs = addFlights(chosen)
        for data in directs:
            direct = data["flyTo"]
            if direct in completed or direct in queue :
                continue
            queue.append(direct)


def addFlights(flyTo):
    print("help")
    url = 'http://localhost:5000/addflights'
    payload = {'flyFrom': 'NYC',
                'flyTo': flyTo,
                'dateFrom': '2019-05-01',
                'dateTo': '2020-01-31' ,
                'exclusions': ''}
    headers = {'content-type': 'application/json'}

    response = requests.post(url, data=json.dumps(payload), headers=headers)
    return response.json()



process()