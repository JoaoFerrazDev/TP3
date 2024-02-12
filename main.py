import asyncio

import aiofiles
import os
temperature_queue = asyncio.Queue()
humidity_queue = asyncio.Queue()
temperature_sum = 0
humidity_sum = 0
MAX_CONCURRENT_FILES = 10


async def process_file(path, temp_queue, hum_queue, sem):
    try:
        async with sem:
            async with aiofiles.open(path) as f:
                async for line in f:
                    try:
                        words = line.split(';')
                        if len(words) == 2:
                            temp, humidity = words
                            await asyncio.wait_for(temp_queue.put(float(temp)), timeout=2)
                            await asyncio.wait_for(hum_queue.put(float(humidity)), timeout=2)
                    except asyncio.TimeoutError:
                        print(f"Timeout reading line from file {path}. Skipping.")
    except FileNotFoundError:
        print(f"File {path} not found.")
    finally:
        os.remove(path)


async def monitor_progress():
    while True:
        num_files = len(os.listdir('C:/Users/joao.ferraz/IdeaProjects/TP3/weather_files'))
        print(f"There are {num_files} files left to process.")
        await asyncio.sleep(1)
        if num_files == 0:
            break


async def process_values(type_of_sum):
    while True:
        if type_of_sum == 1:
            if temperature_queue.qsize() == 0:
                temperature_queue.task_done()
                break
            else:
                value = await temperature_queue.get()
                global temperature_sum
                temperature_sum += value
        elif type_of_sum == 2:
            if humidity_queue.qsize() == 0:
                humidity_queue.task_done()
                break
            else:
                value = await humidity_queue.get()
                global humidity_sum
                humidity_sum += value


async def write_results(temp_sum, hum_sum, total_temp, total_hum):
    with open('tp3_results.txt', 'w') as f:
        average_temperature = temp_sum / total_temp
        average_humidity = hum_sum / total_hum
        f.write(f"Average Temperature: {average_temperature}\n")
        f.write(f"Average Humidity: {average_humidity}\n")
        f.write(f"Total Temperature: {temp_sum}\n")
        f.write(f"Total Humidity: {hum_sum}\n")


async def main():
    directory_path = "C:/Users/joao.ferraz/IdeaProjects/TP3/weather_files"
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FILES)

    file_tasks = []
    task = asyncio.create_task(monitor_progress())
    file_tasks.append(task)
    for filename in os.listdir(directory_path):
        full_path = os.path.join(directory_path, filename)
        task = asyncio.create_task(process_file(full_path, temperature_queue, humidity_queue, semaphore))
        file_tasks.append(task)

    await asyncio.gather(*file_tasks)

    total_temperatures = temperature_queue.qsize()
    total_humidity = humidity_queue.qsize()

    value_tasks = []
    for _ in range(5):
        task = asyncio.create_task(process_values(1))
        value_tasks.append(task)
    for _ in range(5):
        task = asyncio.create_task(process_values(2))
        value_tasks.append(task)

    await asyncio.gather(*value_tasks)
    for i in range(10):
        value_tasks[i].cancel()

    await write_results(temperature_sum, humidity_sum, total_temperatures, total_humidity)


asyncio.run(main())
