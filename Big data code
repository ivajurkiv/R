#upload transactions dataset 
ds_1<-read.table(file.choose(), header=TRUE, sep=",") 
ds_2<-read.table(file.choose(), header=TRUE, sep=",") 
ds_3<-read.table(file.choose(), header=TRUE, sep=",") 
ds_4<-read.table(file.choose(), header=TRUE, sep=",") 
ds_5<-read.table(file.choose(), header=TRUE, sep=",") 
ds_6<-read.table(file.choose(), header=TRUE, sep=",") 
ds_7<-read.table(file.choose(), header=TRUE, sep=",") 
ds_8<-read.table(file.choose(), header=TRUE, sep=",") 
ds_9<-read.table(file.choose(), header=TRUE, sep=",") 
ds_10<-read.table(file.choose(), header=TRUE, sep=",") 
ds_11<-read.table(file.choose(), header=TRUE, sep=",") 
ds_12<-read.table(file.choose(), header=TRUE, sep=",") 

#merge transactions datasets to create one dataset with all values
dataset<-merge(ds_1, ds_2, all=TRUE)
dataset1<-merge(dataset, ds_3, all=TRUE)
dataset2<-merge(dataset1, ds_4, all=TRUE)
dataset3<-merge(dataset2, ds_4, all=TRUE)
dataset4<-merge(dataset3, ds_5, all=TRUE)
dataset5<-merge(dataset4, ds_6, all=TRUE)
dataset6<-merge(dataset5, ds_7, all=TRUE)
dataset7<-merge(dataset6, ds_8, all=TRUE)
dataset8<-merge(dataset7, ds_9, all=TRUE)
dataset9<-merge(dataset8, ds_10, all=TRUE)
dataset10<-merge(dataset9, ds_11, all=TRUE)
ds_trans<-merge(dataset10, ds_12, all=TRUE)


#delete redundant action and payment_amount column 
ds_trans<-subset(dataset, select=-action)
ds_trans<-subset(dataset, select=-payment_amount)

#change the timestamp to only contain month and year
library(lubridate)
a<-year(ds_trans$timestamp_created)
b<-month(ds_trans$timestamp_created)
ds_trans$timestamp<-paste(a,b, sep="-")

#delete remaining redundant columns
ds_trans<-subset(ds_trans, select=-timestamp_created)
ds_trans<-subset(ds_trans, select=-transaction_id)

#convert timestamp to data data type
library(zoo)
dataset$timestamp<-as.Date(as.yearmon(dataset$timestamp))

new_dataset<-dataset%>%
  group_by(user_id)%>%
  dplyr::mutate(first_trans=min(timestamp))

#check for duplicates by user_id
new_dataset<-new_dataset[!duplicated(new_dataset$user_id),]

#delete redunant timestamp value
new_dataset<-subset(new_dataset, select=-timestamp)

#upload user-related dataset
ds_user<-read.table(file.choose(), fill=TRUE, header=TRUE, sep=",")

#replacing company names with 1 and 0 
require(dplyr)
ds_user%>% count(company)
ds_user$company1<-ds_user$company==""
ds_user$company1<-as.numeric(ds_user$company1)
ds_user<-subset(ds_user, select=-company)
names(ds_user)[5]<-"company"

#replace country codes with continents
library(countrycode)
ds_user$full_country<-countrycode(sourcevar=ds_user$address_country, "iso2c", "country.name")
ds_user$continent<-countrycode(sourcevar = ds_user[, "full_country"],
                               origin = "country.name",
                               destination = "continent")

#delete redundant columns  
ds_user<-subset(ds_user, select=-full_country)
dataset_u<-subset(ds_user, select=-timestamp_created)
dataset_u<-subset(dataset_u, select=-address_country)

#merge dataset_u with new_dataset (containing transactions data)
dataset_1_final<-merge(new_dataset, dataset_u, by="user_id")

#delete duplicates
dataset_1_final<-dataset_1_final[!duplicated(dataset_1_final$user_id),]

#upload subscription-related dataset
ds_sub<-read.table(file.choose(), header=TRUE, sep=",") 

#delete duplicates by subscription_id
ds_sub<-ds_sub[!duplicated(ds_sub$subscription_id),]

#delete redundant variable cycle_id
ds_sub<-subset(ds_sub, select=-cycle_id)

#replace NA to 0 for discount code 
ds_sub$discount_code_id[is.na(ds_sub$discount_code_id)]<-0

#replace discount code ids (that are not 0) to 1
ds_sub$discount_code_id[ds_sub$discount_code_id>0]<-1
ds_sub$discount_code_id<-factor(ds_sub$discount_code_id)
names(ds_sub)[9]<-"discount_code"

#count the number of times subscribers returned
str(ds_sub$subscriber_id)
library(plyr)
count<-count(ds_sub$subscriber_id)
return_count<-data.frame(count)
names(return_count)[1]<-"subscriber_id"
names(return_count)[2]<-"count"

#find subscribers who upgraded using description column 
library(stringi)
ds_sub$upgrade<-with(ds_sub, stri_detect_fixed(description, "upgrade"))
ds_sub$upgrade<-as.numeric(ds_sub$upgrade)
ds_sub<-subset(ds_sub, select=-description)

#all users who upgraded
upgraded<-subset(ds_sub, upgrade==1)
#remove duplicate values from upgraded data frame
upgraded<-upgraded[!duplicated(upgraded$subscription_id),]

#all users who did not upgrade
not_upgraded<-subset(ds_sub, upgrade==0)
#remove duplicate values from not_upgraded data frame 
not_upgraded<-not_upgraded[!duplicated(not_upgraded$subscription_id),]

#bind not upgraded and upgraded data frames
ds_sub_f<-rbind(not_upgraded, upgraded)

#load dataset on number of cycles per subscription_id 
data_cycles<-read.table(file.choose(), fill=TRUE, header=TRUE, sep=",")
#merge the loaded dataset with the subscription dataset
ds_sub_final<-merge(ds_sub_f, data_cycles, by="subscription_id")

#delete redundant columns in the new dataset 
ds_sub_final<-subset(ds_sub_final, select=-timestamp_created_subsc)
ds_sub_final<-subset(ds_sub_final, select=-timestamp_created_cycle)
ds_sub_final<-subset(ds_sub_final, select=-timestamp_cancelled_cycle)

#rename column to cycles_count 
names(ds_sub_final)[7]<-"cycles_count"

#calculate mean cycle length 
library(plyr)
avg<-ddply(ds_sub_final, .(subscriber_id),
           summarize, cycles_count=mean(cycles_count))
ds_sub_final<-subset(ds_sub_final, select=-cycles_count)
ds_sub_final1<-merge(ds_sub_final, avg, by="subscriber_id")
names(ds_sub_final1)[7]<-"avg_cycles_count"

#add the number of subscriptions open (return_count data_frame)
ds_sub_final2<-merge(ds_sub_final1, return_count, by="subscriber_id")
names(ds_sub_final2)[8]<-"number_of_subscriptions"

#create a column which tells if user had more than 1 subscription 
ds_sub_final2$multiple_subscriptions<-ds_sub_final2$number_of_subscriptions>1
ds_sub_final2$multiple_subscriptions<-as.numeric(ds_sub_final2$multiple_subscriptions)

#find the latest subscription per subscriber_id
#convert timestamp to data data type
library(zoo)
ds_sub_final2$timestamp_cancelled_subsc<-as.Date(ds_sub_final2$timestamp_cancelled_subsc)

ds_sub_final3<-ds_sub_final2%>%
  group_by(subscriber_id)%>%
  dplyr::mutate(timestamp_cancelled_subsc=max(timestamp_cancelled_subsc))

ds_sub_final3$timestamp_cancelled_subsc<-as.character(ds_sub_final3$timestamp_cancelled_subsc)

#identify churners 
#replace NA to 0 
ds_sub_final3$timestamp_cancelled_subsc[is.na(ds_sub_final3$timestamp_cancelled_subsc)]<-0
ds_sub_final3$timestamp_cancelled_subsc[ds_sub_final3$timestamp_cancelled_subsc>0]<-1

ds_sub_final3$churned<-ds_sub_final3$timestamp_cancelled_subsc==1
ds_sub_final3$churned<-as.numeric(ds_sub_final3$churned)
ds_sub_final3$churned<-as.factor(ds_sub_final3$churned)

#delete redunant timestamp_cancelled column 
ds_sub_final3<-subset(ds_sub_final3, select=-timestamp_cancelled_subsc)

#merge to create final datasetl 
dataset<-merge(dataset_1_final, ds_sub_final3, by="subscriber_id")
dataset<-subset(dataset, select=-user_id)

dataset<-dataset[!duplicated(dataset$subscriber_id),]

count(dataset$churned)

#convert to appropriate data types
str(dataset)
dataset<-subset(dataset, select=-subscriber_id)
dataset$continent<-as.factor(dataset$continent)
dataset$company<-as.factor(dataset$company)
dataset$upgrade<-as.factor(dataset$upgrade)
dataset$number_of_subscriptions<-as.numeric(dataset$number_of_subscriptions)

#format decimal places 
dataset$avg_cycles_count<-round(dataset$avg_cycles_count, digits=0)

#delete subscription id variable
dataset<-subset(dataset, select=-subscription_id)

#export the final data set
write.csv(dataset, "dataset.csv")
