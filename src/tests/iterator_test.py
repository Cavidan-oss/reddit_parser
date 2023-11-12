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



import asyncio

class Test:
    def __init__(self) -> None:
        pass

    async def high_iterator(self):
        yield "test"

        async for low_iter in self.low_iterator():
            yield low_iter

    async def low_iterator(self):
        for i in range(5):
            yield i
            await asyncio.sleep(0.1)  # Simulate asynchronous operation

async def main():
    test = Test()

    async for ite in test.high_iterator():
        print(ite)

if __name__ == "__main__":
    asyncio.run(main())
