from enum import Enum


Date = Enum("date", ["yyyymmdd", "ddmmyyyy", "mmddyyyy"])


def convertdate(Date, select):
    for d in Date:
        if d.value == select:
            date = ""
            result = "".join(dict.fromkeys(d.name))
            l = list(result)
            date = "%"+l[0]+"%"+l[1]+"%"+l[2]
            return date