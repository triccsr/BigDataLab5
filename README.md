## 现在的问题：

### 由于我没认真看ppt,用GraphFileGen实现了任务123的功能，需要把它拆成3部分。

#### 任务5还没写。我看了任务5的论文，算法是：

1. 将每个人物打上一个独有的标签

2. 对于每轮迭代，统计A的所有邻居的标签，选择权重**和**最大的一个作为A的新标签。
   
   + 例如，若A的邻居为 B 0.1|C 0.2|D 0.7，其中B和C的标签为1,D的标签为2,则标签1的权重和为0.3,标签2的权重和为0.7，A的新标签为2

3. 进行若干轮迭代

##### 感觉任务5可以参考任务4的框架。

### 你们两个一个写任务5,一个拆任务123以及写报告？

## 运行方式

#### 任务123：

在hdfs下跑：

yarn jar xxx.jar org.GraphFileGen.GraphFileGen PERSONLIST_TXT_PATH YOUR_INPUT_FOLDER_PATH YOUR_OUTPUT_PATH

在本机跑：

java org.GraphFileGen.GraphFileGen PERSONLIST_TXT_PATH YOUR_INPUT_FOLDER_PATH YOUR_OUTPUT_PATH

#### 任务4

在hdfs下跑：

yarn jar xxx.jar org.PageRank.PageRankMain YOUR_INPUT_FOLDER_PATH YOUR_OUTPUT_PATH  迭代轮数

在本机跑：

java org.PageRank.PageRankMain YOUR_INPUT_FOLDER_PATH YOUR_OUTPUT_PATH 迭代轮数
