class SegmentTree:
    def __init__(self,nums):
        self.n = len(nums)
        self.tree = [0] * (4*self.n)
        self._build(1, 0, self.n-1, nums)
    
    def _build(self, node:int, start:int, end:int, nums:list[int]):
        if start == end:
            self.tree[node] = nums[start]
            return
        
        mid = (start+end)//2

        self._build(node*2, start, mid, nums)
        self._build(node*2+1, mid+1, end, nums)

        self.tree[node] = (self.tree[node*2]+self.tree[node*2+1])
    
    def query(self, left, right):
        return self._query(1, 0, self.n-1, left, right)
    
    def _query(self, node, start, end, left, right):
        if end < left or start > right:
            return 0
        
        if left <= start and end <= right:
            return self.tree[node]
        
        mid = (start+end)// 2

        leftSum = self._query(node*2, start, mid, left, right)
        rightSum = self._query(node*2+1, mid+1, end, left, right)

        return leftSum+rightSum
    
    def update(self, index, value):
        self._update()

        


