package org.apache.druid.client.cache.ORF.online.randomforest;

import java.util.Arrays;

import org.apache.druid.client.cache.ORF.Config;
import org.apache.druid.client.cache.ORF.online.Classifier;
import org.apache.druid.client.cache.ORF.structure.Result;
import org.apache.druid.client.cache.ORF.structure.Sample;
import org.apache.druid.java.util.common.logger.Logger;


public class RandomTree implements Classifier {
    private static final Logger log = new Logger(RandomTree.class);
    private RandomNode rootNode;

    RandomTree(Config config, int numClasses, int numFeatures,
               double[] minFeatRange, double[] maxFeatRange, int[] featureRange) {
        // Layout layout = new PatternLayout("%-d{yyyy-MM-dd HH:mm:ss}  [ %l:%r ms ] - [ %p ]  %m%n");
        // Appender appender = new FileAppender(layout,"G://druid_code/insertion module/ORFCache/logs/test.log");
        // log.addAppender(appender);
        this.rootNode = new RandomNode(config, numClasses, numFeatures, minFeatRange, maxFeatRange, 0, featureRange);
    }

    @Override
    public void update(Sample sample) {
        this.rootNode.update(sample);
    }

    @Override
    public void eval(Sample sample, Result result) {
        this.rootNode.eval(sample, result);
    }

    /**
     * 将二叉树的信息打印出来
    树的结构示例：
              1
            /   \
          2       3
         / \     / \
        4   5   6   7
    */

    public int getTreeDepth(RandomNode root) {
        return root == null ? 0 : (1 + Math.max(getTreeDepth(root.leftChildNode), getTreeDepth(root.rightChildNode)));
    }


    private void writeArray(RandomNode currNode, int rowIndex, int columnIndex, String[][] res, int treeDepth) {
        // 保证输入的树不为空
        if (currNode == null) return;
        // 先将当前节点保存到二维数组中  保存的信息
        int splitFeatureId =-1;
        double splitFeatureThreshold=-1;
        if(currNode.getBestTest()!=null){
            splitFeatureId = currNode.getBestTest().getFeatureId();
            splitFeatureThreshold = currNode.getBestTest().getThreshold();
        }
        res[rowIndex][columnIndex] = "("+Arrays.toString(currNode.labelStats)+" "+
            (currNode.counter + currNode.parentCounter) +" "+ currNode.label +" "+currNode.isLeaf+" "+splitFeatureId+" "+splitFeatureThreshold+")";
        //res[rowIndex][columnIndex] = Arrays.toString(currNode.labelStats);
        //log.error("writeArray:   "+res[rowIndex][columnIndex]+"  treeDepth:"+treeDepth);

        // 计算当前位于树的第几层
        int currLevel = ((rowIndex + 1) / 2);
        // 若到了最后一层，则返回
        if (currLevel == treeDepth) return;
        // 计算当前行到下一行，每个元素之间的间隔（下一行的列索引与当前元素的列索引之间的间隔）
        int gap = treeDepth - currLevel - 1;

        // 对左儿子进行判断，若有左儿子，则记录相应的"/"与左儿子的值
        if (currNode.leftChildNode != null) {
            res[rowIndex + 1][columnIndex - gap] = "/";
            //log.error("writeArray:   "+res[rowIndex + 1][columnIndex - gap]+"  treeDepth:"+treeDepth);
            writeArray(currNode.leftChildNode, rowIndex + 2, columnIndex - gap * 2, res, treeDepth);
        }

        // 对右儿子进行判断，若有右儿子，则记录相应的"\"与右儿子的值
        if (currNode.rightChildNode != null) {
            res[rowIndex + 1][columnIndex + gap] = "\\";
            //log.error("writeArray:   "+res[rowIndex + 1][columnIndex + gap]+"  treeDepth:"+treeDepth);
            writeArray(currNode.rightChildNode, rowIndex + 2, columnIndex + gap * 2, res, treeDepth);
        }
    }


    public void show() {
        RandomNode root = this.rootNode;
        if (root == null)
            return;
        // 得到树的深度
        int treeDepth = getTreeDepth(root);

        // 最后一行的宽度为2的（n - 1）次方乘3，再加1
        // 作为整个二维数组的宽度
        int arrayHeight = treeDepth * 2 - 1;
        int arrayWidth = (2 << (treeDepth - 2)) * 3 + 1;
        // 用一个字符串数组来存储每个位置应显示的元素
        String[][] res = new String[arrayHeight][arrayWidth];
        // 对数组进行初始化，默认为一个空格
        for (int i = 0; i < arrayHeight; i ++) {
            for (int j = 0; j < arrayWidth; j ++) {
                res[i][j] = " ";
            }
        }

        // 从根节点开始，递归处理整个树
        // res[0][(arrayWidth + 1)/ 2] = (char)(root.val + '0');
        writeArray(root, 0, arrayWidth/ 2, res, treeDepth);



        for (String[] line: res) {
            StringBuilder sb = new StringBuilder();
            for (String s : line) {
                sb.append(s);
                /*if (line[j].length() > 1 && j <= line.length - 1) {
                    j += line[j].length() > 4 ? 2 : line[j].length() - 1;
                }*/
            }
            log.error(sb.toString());
        }


        //return res;
        // 此时，已经将所有需要显示的元素储存到了二维数组中，将其拼接并打印即可
    }


}
