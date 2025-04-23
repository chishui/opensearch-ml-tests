import os
import json
import uuid
import time
import random
import unittest
import itertools
from basic import client
from basic.pipeline import Pipeline
from basic.ingest import Ingest
from basic.doc import Doc
from basic.cluster import Cluster
from basic.util import parser


class Jvm(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(Jvm, self).__init__(*args, **kwargs)
        self.cluster = Cluster()

    def test_check_jvm(self):
        mem_use = []
        sampling_count = 6
        for i in range(sampling_count):
            res = self.cluster.stats.jvm()
            for k in res["nodes"]:
                mem_use.append(int(res["nodes"][k]["jvm"]["mem"]["heap_used_in_bytes"])) 
            time.sleep(5)
        print(f'heap_used_in_bytes: {sum(mem_use)/len(mem_use)}')
        self.assertTrue(False)


