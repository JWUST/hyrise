import random
import sys

f = open(sys.argv[1], 'w+')
f.write("column0\nINTEGER\n0_C\n===\n")
i = 0
while i < 1000000:
#while i < 20:
    f.write("{0}\n".format(random.randint(0, 19999)))
    i += 1
f.close()
