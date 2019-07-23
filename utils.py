import locale

locale.setlocale(locale.LC_ALL,'pt_PT.UTF-8')

def binary_search(arr,l,r,x):

    if r>= l:
        # Find mid element
        mid = l + (r-l) // 2
        mid_elem = arr[mid]

        if mid_elem[0] == x:
            return mid
        elif locale.strcoll(mid_elem[0],x)>0:
            return binary_search(arr, l, mid-1, x)
        else:
            return binary_search(arr, mid+1, r, x)
    else:
        return -1

def merge_sort(arr): 

    if len(arr) > 1: 
        mid = len(arr)//2   # Finding the mid of the array 
        L = arr[:mid]       # Dividing the array elements  
        R = arr[mid:]       # into 2 halves 
  
        merge_sort(L)       # Sorting the first half 
        merge_sort(R)       # Sorting the second half 
  
        i = j = k = 0
          
        # Copy data to temp arrays L[] and R[] 
        while i < len(L) and j < len(R):
           
            if locale.strcoll(L[i][0],R[j][0])<0 : 
                arr[k] = L[i] 
                i+=1
            else: 
                arr[k] = R[j] 
                j+=1
            k+=1
          
        # Checking if any element was left 
        while i < len(L): 
            arr[k] = L[i] 
            i+=1
            k+=1
          
        while j < len(R): 
            arr[k] = R[j] 
            j+=1
            k+=1
  

