## Task1

### 输入

+ 文件夹，内有未分词的小说
+ 人物名字列表，包括全名和简称

### 输出

+ 提取出每段话出现的人名

### 实现

**由于我在任务1中就进行了人名统一，我的任务1的输出格式形如 PERSON_A,COUNT_A;PERSON_B,COUNT_B;...(e.g Harry,2;Ron 3;Hermione 1;)**

#### Mapper.setup()

`setup`的任务是建立人物全名集合,以及人物简称到一系列全名的映射表。

我使用`context.getLocalCacheFile()`来读取人名列表。按单词的数量从大到小的顺序遍历人名列表，若一个人名的单词数为1且存在一个已经遍历到的人名包含它，则将该人名归类为简称，否则归类为全名。此处时间复杂度$\Theta(人名表长 \cdot log(人名表长))$

一个简称可能对应不止一个全名（例如姓氏，人名列表中的Potter被视为简称，它对应的全名有Harry Potter, James Potter和Lily Potter），若一个全名包含该简称，则该全名在该简称对应的全名集合中。简称到全名集合的映射表可以通过对每个简称，遍历所有全名的方式进行,总时间复杂度为$\Theta(人名表长^2)$ 。虽然也有$\Theta(nlogn)$（n为人名表长）的做法，但由于人名列表的长度只有240, 此处不是时间瓶颈。

### Mapper.map()

**我对匹配到的人物简称进行了特殊处理，以加强统计的准确性。**

`map(Object key,Text value, Context context)`，其中`value`的值为原小说的某一段落。

将value转换成字符串，暴力匹配人名。

+ 若匹配到了全名，则直接将此人的计数器+1

+ 若匹配到了简称
  
  + 若简称只对应一个全名，则将对应的人的计数器+1
  
  + 否则将该简称放入waitlist中

然后遍历waitlist，若该简称对应的全部人名中**有且仅有一个**出现在了此段落中，则视作该简称指代这个唯一出现过的人，将此人的计数器+1（例如: Ron Weasley says to Harry Potter: "Hello, Potter!" ，此处的Potter应指代Harry Potter而非James Potter或Lily Potter）。否则（都未出现或出现不止一个人）无法判断该简称指谁，计数器都不变。

最后将该段落出现的所有人以及出现的次数写入`context`，形如PERSON_A,COUNT_A;PERSON_B,COUNT_B;...(e.g Harry,2;Ron 3;Hermione 1;)

时间复杂度$\Theta(nlogp)$，其中n为段落长度，p为人物表长度。

### Reducer

Mapper干了绝大部分的活，Reducer无需特殊处理。

### 运行结果：

![](/home/triccsr/.config/marktext/images/2023-07-16-01-47-29-image.png)

![](/home/triccsr/.config/marktext/images/2023-07-16-01-51-29-image.png)

## Task3

任务比较简单。我新建了private class NeighborWritable，域为to（人物）和count（出现次数）。

在mapper中，对于输入`<Person_A,Person_B>Count`，`context.write(Person_A,new NeighborWritable(Person_B,Count))`。

在reducer中，统计每个人与所有其他人共现的总次数，将每个人的共现次数除以总次数，得到每个人的比例。

### 运行结果：

![](/home/triccsr/.config/marktext/images/2023-07-16-01-54-15-image.png)

![](/home/triccsr/.config/marktext/images/2023-07-16-01-54-57-image.png)

## Task4

Task4分为3个Mapreduce模块，Init,Work和Main

### Init

Init为对task3输出的图的简单格式处理。

task3的输出格式为 `Person [NeighborA, proportionA|NeighborB, proportionB|...]`

Init后的格式为 Person  ;pageRank值(初始为$\frac{1}{PersonCount}$) ; 总人数;NeighborA, proportionA,NeighborB,proportionB,...

### Work

work为一次pagerank迭代。

#### Map

PageRank算法的公式为$PRVal[u]=\frac{1-D}{N}+D \cdot \displaystyle \sum_{v \in pred(u)}PRVal[v]\cdot proportion(v,u)$ ,转换成MapReduce的写法就是`for w : successors of u, PRVal[w] += D * PRVal[u] * proportion(u,w)`

对于每条value，形如`Person  ;PRVal;N;NeighborA,proportionA;NeighborB,proportionB,...`

1. 遍历所有的`Neighbor`，对每个Neighbor执行`context.write(Now_Neighbor,"+"+ D * PRVal * Now_proportion)`。

2. **将这行value原封不动地写入`context`，key值为这条value对应的Person。**（因为我们需要在输出中保留这张图）
   
   #### Combine
+ 若value以"+"开头，则说明是PRVal的计算信息，统计和。
+ 否则是此人的信息列表，直接写入context
+ 最后将所有PRVal计算信息的和写入context

#### Reduce

将key对应的人的PRVal初始值设为0

遍历所有value：

+ 若遇到以"+"开头的value,说明这个value是关于此人PRVal的计算信息（即Map中的1.），将此人的PRVal加上该值。

+ 否则，说明这个value是此人的信息列表（即Map中的2.），暂时记录，并且读取N的值。

将PRVal加上$\frac{1-D}{N}$，修改此人的信息列表，输出到context

### Main

含有排序MapReduce。

主要功能是控制流：先执行init，然后按照传进来的参数迭代work,最后执行排序MapReduce.

### 在平台上迭代3次的运行结果：

![](https://raw.githubusercontent.com/triccsr/myPicBed/main/202307161226304.png)
这是平台跑5轮的结果，可以看出与迭代3轮的结果相差不多：
![](https://raw.githubusercontent.com/triccsr/myPicBed/main/202307161221442.png)

**问题：迭代5轮时有时平台报错，同样的程序每次跑报的错还不一样）**

有时是超时（这个任务的时间复杂度不高，只有遍历的时间复杂度，Combiner也加了，但还是超时）：

![](https://raw.githubusercontent.com/triccsr/myPicBed/main/202307161148311.png)

有时是No Route to Host:

![](https://raw.githubusercontent.com/triccsr/myPicBed/main/202307161156451.png)

