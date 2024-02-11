import os
import asyncio
import aiofiles


async def worker(name, queue):
    while True:
        # Get a "work item" out of the queue.
        sleep_for = await queue.get()

        # Sleep for the "sleep_for" seconds.
        await asyncio.sleep(sleep_for)

        # Notify the queue that the "work item" has been processed.
        queue.task_done()

        print(f"{name} has slept for {sleep_for:.2f} seconds.")


async def test_queue():
    # Create a queue that we will use to store our "workload".
    queue = asyncio.Queue()

    # Create three worker tasks to process the queue concurrently.
    tasks = []
    for i in range(3):
        task = asyncio.create_task(worker(f"Worker-{i}", queue))
        tasks.append(task)

    # Wait until the queue is fully processed.
    started_at = time.monotonic()
    await queue.join()
    total_slept_for = time.monotonic() - started_at

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled.
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("All worker tasks were cancelled.")

    print(f"3 workers slept in parallel for {total_slept_for:.2f} seconds.")
    print(f"Total expected sleep time: {total_sleep_time:.2f} seconds.")


async def process_file(filename):
    async with aiofiles.open(filename, mode='r') as f:
        async for line in f:
            words = line.strip().split(';')
            if len(words) == 2:
                word1, word2 = words
                print(f"First word: {word1}, Second word: {word2}")


async def read_files_in_directory(directory):
    tasks = []
    async for file in aiofiles.os.listdir(directory):
        filename = os.path.join(directory, file)
        if os.path.isfile(filename):
            for i in range(3):
                task = asyncio.create_task(process_file(filename))
                tasks.append(task)

        tasks.append(process_file(filename))
    await asyncio.gather(*tasks)


def process_data(filename):
    queue_for_temperature = asyncio.Queue()
    queue_for_humidity = asyncio.Queue()

    queue.put_nowait(sleep_for)

    async with aiofiles.open(filename, mode='r') as f:
        async for line in f:
            words = line.strip().split(';')
            if len(words) == 2:
                word1, word2 = words
                print(f"First word: {word1}, Second word: {word2}")
    os.remove(filename)


async def main():
    directory = ""
    asyncio.run(read_files_in_directory(directory))


if __name__ == "__main__":
    main()
