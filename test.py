import asyncio
import time

test = "test"

async def sleep():
    print(f'Time: {time.time() - start:.2f}')
    time.sleep(1)

async def sum(name, numbers):
    total = 0
    for number in numbers:
        print(f'Task {name}: Computing {total}+{number}')
        await sleep()
        total += number
    print(f'Task {name}: Sum = {total}\n')

start = time.time()
asyncio.run(sum("A", [1, 2]))
asyncio.run(sum("B", [1, 2, 3]))

# loop = asyncio.get_event_loop()
# task1 = loop.create_task(sum("A", [1, 2]))
# task2 = loop.create_task(sum("B", [1, 2, 3]))
# tasks = [
#     task1, task2,
# ]
# loop.run_until_complete(asyncio.wait(tasks))
# loop.close()

end = time.time()
print(f'Time: {end-start:.2f} sec')