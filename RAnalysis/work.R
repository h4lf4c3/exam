# 导入数据
data <- read.table("./data.csv",sep=",")

# 时间序列图
print(data)
data <- data.frame(data)
plot_data <- ts(data[,2],start=1)

plot(plot_data,type="o",xlab="月",ylab="销售量",main="梅沙饭店销售量",col="blue")

# 求季节指数
sales <- data[,2]  #销量
#导入forecast包
library("forecast")
# 进行中心平均
centreAvg <- ma(sales,3,centre=T)

# 趋势剔除值
trendAdj <- sales / centreAvg

# 得到每年的数据
year_data <- data.frame(trendAdj[1:12],trendAdj[13:24],trendAdj[25:36])
names(year_data) <- c("第一年","第二年","第三年")

year_data$sum <- year_data[,1]+year_data[,2]+year_data[,3]
year_data[1,4] <- year_data[1,2] + year_data[1,3]
year_data[12,4] <- year_data[12,1] + year_data[12,2]

year_data$avg <- year_data$sum/3
year_data[1,5] <- year_data[1,4]/2
year_data[12,5]<- year_data[12,4]/2

# 季节指数
year_data$seasonal <- year_data$sum/(sum(year_data$sum)/12)
plot(year_data$seasonal,type = "o",xlab = "月",ylab = "季节指数")

# 指数平滑法预测
# 定义指数平滑模型
smooth <- function(x, alpha = 0.3){
  n <- length(x); f <- x[1]
  for (t in 1:n){
    f[t+1] <-  (1-alpha)*x[t+1]+alpha*f[t]
  }
  mse <- sum((x-f[1:n])^2)/(n-1)
  list(average = f, MSE = mse)
}

# 分别计算一次和二次指数平滑
es <- smooth(sales)
ses <- smooth(es$average)

es <- es$average[1:35]
ses <- ses$average[1:34]

# 求截距a
a <- 2*es[1:34] - ses[1:34]
# 求斜率b
b <- 3*(es[1:34]-ses[1:34])/7

# 趋势预测值
y <- a+b

# 预测值为
pre <- numeric(0)
for(j in 1:14){
  pre[j] <- a[34]+b[34]*j
}

# 美化输出
pre[3:14]
month <- c("一月","二月","三月","四月","五月","六月","七月","八月","九月","十月"
           ,"十一月","十二月")
fin_data <- data.frame(month,pre[3:14])

