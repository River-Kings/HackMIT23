import json

skyEventCount = 0
eventTypeCountAggregation = {}

with open("dawnEventDataStream.txt", "r", encoding="utf-8") as file:
    line_count = 0
    for line in file:
        if line_count >= 1000:
            break

        if line.startswith("[") or line.startswith("]"):
            continue
        
        cleaned_line = line.strip().rstrip(',')
        data = json.loads(cleaned_line)
        if "events" in data:
            events_data = data["events"]
            skyEventCount += 1
            for key, count in events_data.items():
                if key not in eventTypeCountAggregation:
                    eventTypeCountAggregation[key] = 0
                eventTypeCountAggregation[key] += count
        
        line_count += 1

result = {"skyEventCount": skyEventCount, "eventTypeCountAggregation": eventTypeCountAggregation}

print(f"Read the first 1000 lines. Result is {json.dumps(result, indent=2)}")
