1．需求
统计过程中对每一个MapTask的输出进行局部汇总，以减小网络传输量即采用Combiner功能。(统计单词出现的个数)
（1）数据输入
banzhang ni hao
xihuan hadoop banzhang
banzhang ni hao
xihuan hadoop banzhang

（2）期望输出数据
期望：Combine输入数据多，输出时经过合并，输出数据降低。

不添加combiner汇总时，reducer接收到的map数据如下：
    banzhang 1
    ni 1
    hao 1
    xihuan 1
    ...
    xihuan 1
    hadoop 1
    banzhang 1
  一共有12条数据需要传输。

 添加combiner汇总后，reducer接收到的map数据如下：
    banzhang 4
    ni 2
    hao 2
    xihuan 2
    hadoop 2
 一共有5条数据需要传输，减少了数据传输消耗。



