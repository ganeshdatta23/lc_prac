class solution():
    def twosum(self, nums, target):
        d={}
        res=[]
        nums=sorted(nums)
        for i, j in enumerate(nums):
            print(i,j,"---------")
            if j in d:
                inx=d[j]
                res.append([inx, i])
            d[target-j]=i
        if res:            return res

        return []

if __name__ == "__main__":
    s = solution()
    print(s.twosum([2, 7,1,8,0,9, 11, 15], 9))
