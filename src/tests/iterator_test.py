# class Test:
#     def __init__(self) -> None:
#         pass

#     def high_iterator(self):
#         yield "test"

#         for low_iter  in self.low_iterator():
#             yield low_iter

#     def low_iterator(self):
#         for i in range(5):
#             yield i

# test = Test()

# for ite in test.high_iterator():
#     print(ite)



# import asyncio

# class Test:
#     def __init__(self) -> None:
#         pass

#     async def high_iterator(self):
#         yield "test"

#         async for low_iter in self.low_iterator():
#             yield low_iter

#     async def low_iterator(self):
#         for i in range(5):
#             yield i
#             await asyncio.sleep(0.1)  # Simulate asynchronous operation

# async def main():
#     test = Test()

#     async for ite in test.high_iterator():
#         print(ite)

# if __name__ == "__main__":
#     asyncio.run(main())
import asyncio

class AsyncIteratorWrapper:
    def __init__(self, async_generator):
        self._async_generator = async_generator
        self._task = None

    async def __anext__(self):
        if self._task is None:
            self._task = asyncio.create_task(self._async_generator.__anext__())
        elif self._task.done():
            if self._task.result() is None:
                raise StopAsyncIteration
            self._task = asyncio.create_task(self._async_generator.__anext__())

        return await self._task

class AsyncConcurrentIterator:
    def __init__(self, async_generators):
        self._async_generators = async_generators
        self._async_iterator_wrappers = []
        for async_generator in async_generators:
            self._async_iterator_wrappers.append(AsyncIteratorWrapper(async_generator))

    async def __anext__(self):
        while True:
            for async_iterator_wrapper in self._async_iterator_wrappers:
                try:
                    result = await async_iterator_wrapper.__anext__()
                    yield result
                except StopAsyncIteration:
                    self._async_iterator_wrappers.remove(async_iterator_wrapper)
            if not self._async_iterator_wrappers:
                raise StopAsyncIteration

async def async_generator_1():
    for i in range(10):
        await asyncio.sleep(0.1)
        yield i

async def async_generator_2():
    for i in range(10, 20):
        await asyncio.sleep(0.1)
        yield i

async def main():
    async_iterator = AsyncConcurrentIterator([async_generator_1(), async_generator_2()])

    async for result in async_iterator:
        print(result)

if __name__ == '__main__':
    asyncio.run(main())