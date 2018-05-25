
def calc_next(targetStr):
    nexts = [-1,0]
    for dumi,ch in enumerate(targetStr):
        if dumi==0:
            continue
        if dumi==1:
            nexts.append(1 if targetStr[0]==targetStr[1] else 0)
            continue
        checkPos = dumi
        findnum = 0
        while checkPos>0:
            if targetStr[nexts[checkPos]] == ch:
                findnum = nexts[checkPos] + 1
                break
            else:
                checkPos = nexts[checkPos]
        nexts.append(findnum)
    return nexts

def kmp(searchStr,targetStr):
    nexts = calc_next(targetStr=targetStr)
    searchNum = len(searchStr)
    targetNum = len(targetStr)
    searchPos = 0
    foundNum = 0
    head = -1
    for dumi,ch in enumerate(searchStr):

        while searchPos>-1 and targetStr[searchPos]!=ch:
            searchPos = nexts[searchPos]

        foundNum = searchPos + 1
        print(foundNum)

        if foundNum==targetNum:
            head = dumi + 1 - searchNum
            print(head)
            break

    return head

if __name__=='__main__':
    print(calc_next('111'))
    print(calc_next('ababaca'))
    print(kmp('123456','123'))