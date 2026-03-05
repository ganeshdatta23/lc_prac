import time

class Solution:
    def twoSumAll(self, nums, target):
        seen = {}
        result = []   # indices
        pairs = []    # actual numbers

        for i, num in enumerate(nums):
            if num in seen:
                idx = seen[num]
                print(idx)
                print("****")
                print(i)
                result.append([idx, i])
                print(result)
                pairs.append([nums[idx], num])  # store the numbers themselves
            seen[target - num] = i

        return result, pairs

# Example input
nums = [2,9,0,7,1,8,11,15]
target = 9

sol = Solution()
start_time = time.perf_counter()
output_indices, output_values = sol.twoSumAll(nums, target)
elapsed_ms = (time.perf_counter() - start_time) * 1000

print("Indices of pairs:", output_indices)
print("Values of pairs:", output_values)
print(f"Execution time: {elapsed_ms:.6f} ms")

# class Solution:
#     def twoSum(self, nums, target):
#         seen = {}
#         for i, num in enumerate(nums):
#             if num in seen:
#                 return [seen[num], i]
#             seen[target - num] = i



# nums = [2,9,0,7,1,8,11,15]
# target = 9

# sol = Solution()
# print(sol.twoSum(nums, target))
