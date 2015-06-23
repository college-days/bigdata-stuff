package org.give.altc

/**
 * Created by zjh on 15-4-9.
 */

object PathNamespace extends Serializable{
    //hdfs上最开始的路径前缀
    val prefix: String = "hdfs://namenode:9000/givedata/altc/"

    //离线测试
    val offlineprefix: String = prefix + "offline/"
    val yaofflineprefix: String = prefix + "yaoffline/"

    val metadataprefix: String = prefix + "metadata/"

    //11.18-12.17
    val offlinefeaturedata: String = metadataprefix + "offlinefeaturedata"
    //11.19-12.18
    val onlinefeaturedata: String = metadataprefix + "onlinefeaturedata"
    //12.18
    val offlinetraindata: String = metadataprefix + "offlinetraindata"

    //离线测试不要再73分了
    //11.18-12.16提取离线训练数据 12.17offlinetrainfeaturedata作为label
    val offlinetrainfeaturedata: String = metadataprefix + "offlinetrainfeaturedata"
    //11.19.12.17提取离线测试数据 12.18作为label
    val offlinetestfeaturedata: String = metadataprefix + "offlinetestfeaturedata"
    //12.17的target数据
    val offlinetrainlabeldata: String = metadataprefix + "offlinetrainlabeldata"
    //12.18的target数据
    val offlinetestlabeldata: String = metadataprefix + "offlinetestlabeldata"
    //11.20-12.18作为线上的训练数据
    val onlinetrainfeaturedata: String = metadataprefix + "onlinetrainfeaturedata"

    //子集的数据集
    //11.18-12.16提取离线训练数据 12.17offlinetrainfeaturedata作为label
    //useritem的特征数据
    val offlinetrainfeaturesubdata: String = metadataprefix + "offlinetrainfeaturesubdata"
    //11.19.12.17提取离线测试数据 12.18作为label
    val offlinetestfeaturesubdata: String = metadataprefix + "offlinetestfeaturesubdata"
    //12.17的target数据
    val offlinetrainlabelsubdata: String = metadataprefix + "offlinetrainlabelsubdata"
    //12.18的target数据
    val offlinetestlabelsubdata: String = metadataprefix + "offlinetestlabelsubdata"
    //11.20-12.18作为线上的训练数据
    val onlinetrainfeaturesubdata: String = metadataprefix + "onlinetrainfeaturesubdata"

    //useritem的特征数据
    val offlineuseritemtrainfeaturesubdata: String = prefix + "subset/offlinetrainfeatures"
    val offlineuseritemtestfeaturesubdata: String = prefix + "subset/offlinetestfeatures"
    val onlineuseritemtrainfeaturesubdata: String = prefix + "subset/onlinetrainfeatures"

    //user的特征数据
    val offlineusertrainfeaturesubdata: String = prefix + "subset/offlineusertrainfeaturesubdata"
    val offlineusertestfeaturesubdata: String = prefix + "subset/offlineusertestfeaturesubdata"
    val onlineusertrainfeaturesubdata: String = prefix + "subset/onlineusertrainfeaturesubdata"

    //item的特征数据
    val offlineitemtrainfeaturesubdata: String = prefix + "subset/offlineitemtrainfeaturesubdata"
    val offlineitemtestfeaturesubdata: String = prefix + "subset/offlineitemtestfeaturesubdata"
    val onlineitemtrainfeaturesubdata: String = prefix + "subset/onlineitemtrainfeaturesubdata"

    //useritem user item join在一起的所有的特征数据
    val offlinealltrainfeaturesubdata: String = prefix + "subset/offlinealltrainfeatures"
    val offlinealltestfeaturesubdata: String = prefix + "subset/offlinealltestfeatures"
    val onlinealltrainfeaturesubdata: String = prefix + "subset/onlinealltrainfeatures"

    //新的数据集 直接在子集数据上划分数据
    val newprefix: String = "hdfs://namenode:9000/givedata/altc/newdata"
    val newmetadataprefix: String = newprefix + "metadata/"
    val newfeatureprefix: String = newprefix + "features/"

    //子集的数据集
    //11.18-12.16提取离线训练数据 12.17offlinetrainfeaturedata作为label
    //useritem的特征数据
    val newofflinetrainfeaturesubdata: String = newmetadataprefix + "offlinetrainfeaturesubdata"
    //11.19.12.17提取离线测试数据 12.18作为label
    val newofflinetestfeaturesubdata: String = newmetadataprefix + "offlinetestfeaturesubdata"
    //12.17的target数据
    val newofflinetrainlabelsubdata: String = newmetadataprefix + "offlinetrainlabelsubdata"
    //12.18的target数据
    val newofflinetestlabelsubdata: String = newmetadataprefix + "offlinetestlabelsubdata"
    //11.20-12.18作为线上的训练数据
    val newonlinetrainfeaturesubdata: String = newmetadataprefix + "onlinetrainfeaturesubdata"

    //useritem的特征数据
    val newofflineuseritemtrainfeaturesubdata: String = newfeatureprefix + "offlineuseritemtrainfeatures"
    val newofflineuseritemtestfeaturesubdata: String = newfeatureprefix + "offlineuseritemtestfeatures"
    val newonlineuseritemtrainfeaturesubdata: String = newfeatureprefix + "onlineuseritemtrainfeatures"

    //user的特征数据
    val newofflineusertrainfeaturesubdata: String = newfeatureprefix + "offlineusertrainfeaturesubdata"
    val newofflineusertestfeaturesubdata: String = newfeatureprefix + "offlineusertestfeaturesubdata"
    val newonlineusertrainfeaturesubdata: String = newfeatureprefix + "onlineusertrainfeaturesubdata"

    //item的特征数据
    val newofflineitemtrainfeaturesubdata: String = newfeatureprefix + "offlineitemtrainfeaturesubdata"
    val newofflineitemtestfeaturesubdata: String = newfeatureprefix + "offlineitemtestfeaturesubdata"
    val newonlineitemtrainfeaturesubdata: String = newfeatureprefix + "onlineitemtrainfeaturesubdata"

    //useritem user item join在一起的所有的特征数据
    val newofflinealltrainfeaturesubdata: String = newfeatureprefix + "offlinealltrainfeatures"
    val newofflinealltestfeaturesubdata: String = newfeatureprefix + "offlinealltestfeatures"
    val newonlinealltrainfeaturesubdata: String = newfeatureprefix + "onlinealltrainfeatures"

    //最终提交
    val cleanthaprefix: String = prefix + "cleantha/"

    val tianchi_mobile_recommend_train_item: String = prefix + "tianchi_mobile_recommend_train_item.csv"
    val tianchi_mobile_recommend_train_user: String = prefix + "tianchi_mobile_recommend_train_user.csv"
}
