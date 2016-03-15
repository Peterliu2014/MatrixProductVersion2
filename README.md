# MatrixProductVersion2
map()和reduce()中无意识创建对象的部分增加了系统开销，导致MR程序运行速度缓慢，将需要创建对象的放在setup()中之后，实验效果很好
