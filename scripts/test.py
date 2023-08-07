

import argparse
ap = argparse.ArgumentParser()
ap.add_argument("-d", "--date", required = True, help="date")
ap.add_argument("-i", "--content", required = True, help="content", nargs='+')
args = vars(ap.parse_args())

print(str(args["content"]))
print(str(" ".join(args["content"])))
