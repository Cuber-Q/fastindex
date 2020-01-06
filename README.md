# fastindex
A fast index about huge data file

Pre-handle the data file and fetch each k-v pair, saving the <key, value_size, value_posiont> as a index item. Saving them to 
index files by sharding key. There will be a lot of small index files, which are in a same level. 


When we create fastindex, for example: k=101 and v=abc, we will calculate the shard value by k % 1000 and it's 1, so we save this k-v pair's index 
into index_1.idx file.

When we find a k-v, fastindex will calculcate the shard-value of index,such as key % 1000, after that we get a index file. 
Using binary search algorithm to get the value_size and value_postion, and read <value_size> byte from the data file at the give offset:<value_postion>.

Index files will be mapped into memory using `mmap` when find k-v.

Usage:
```
Usage of fastindex:
  -cmd string
    	createData: create data file; createIndex: create indexFile; findTest: testing find k-v
  -dir string
    	specify the base dir
  -size string
    	specify the dataSize, such as: 4M, 16G, 128G, 1T (default "16G")
```

You can change the dir as you need.

Creating data file:
```
./fastindex -cmd createData -size 16G -dir /Users/Cuber_Q/goproj/fastindex
```

Creating index file:
```
./fastindex -cmd createIndex -dir /Users/Cuber_Q/goproj/fastindex
```

Finding test:
```
./fastindex -cmd findTest -dir /Users/Cuber_Q/goproj/fastindex
```
