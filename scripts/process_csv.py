results = dict()
f = open("lengths.csv", "r")
lines = f.readlines(2**20)
while lines:
  print lines[0].rstrip("\n")
  for line in lines:
    cleanSplit = line.rstrip("\n").split(",")
    expected = int(cleanSplit[2])
    actual = int(cleanSplit[3])
    if expected not in results: results[expected] = dict()
    if actual not in results[expected]: results[expected][actual] = 0
    results[expected][actual] += 1
  lines = f.readlines(2**20)
f.close()

with open("results.txt", "w") as f:
  for x in results:
    for y in results[x]:
      result = "{} {} {}\n".format(x, y, results[x][y])
      print result.rstrip("\n")
      f.write(result)

