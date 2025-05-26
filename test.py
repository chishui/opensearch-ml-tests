import ray

ray.init(num_cpus=1)

@ray.remote
def add(a, b):
    print(a+b)


for i in range(100):
    add.remote(i, i+1)