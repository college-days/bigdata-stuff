package org.give.nlp;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.util.FilterModifWord;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zjh on 14-10-14.
 */
public class BasicSplit {
    public static void getStopWords() throws IOException {
        StringBuffer buffer = new StringBuffer();
        String filePath = "data/stopwords.dic";
        InputStream is = new FileInputStream(filePath);
        String line; // 用来保存每行读取的内容
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        line = reader.readLine(); // 读取第一行
        while (line != null) { // 如果 line 为空说明读完了
            System.out.println(line);
            buffer.append(line); // 将读到的内容添加到 buffer 中
            FilterModifWord.insertStopWord(line);
            //buffer.append("\n"); // 添加换行符
            line = reader.readLine(); // 读取下一行
        }
        reader.close();
        is.close();
        System.out.println(buffer.length());
        System.out.println(buffer.toString());
    }

    public static void splitWordForUsers(String inputPath, String outputPath){

    }

    public static void splitWordOnNewsContent(String inputPath, String outputPath, boolean throwEmptyWordAway) throws IOException{
        String[] noMeaningWords = {"x", "zg", "uj", "ul", "e", "d", "uz", "y", "w", "en"};
        List<String> noMeaningWordList = Arrays.asList(noMeaningWords);
        System.out.println(noMeaningWordList.contains("uj"));
        Set<String> splitWordSet = new HashSet<String>();

        String[] yaNoMeaningWords = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "%", "'", ";", ",", "-", ".", "/", ":", "?", ">", "<", "=", "@", "[", "]", "~", " ", "|", "ê", "é", "è", "á", "à", "\"", "#", "&", "(", ")", "*", "$", "\\", "_", "°", "í", "\t", "ü", "ú", "+", "!"};
        List<String> yaNoMeaningWordList = Arrays.asList(yaNoMeaningWords);

        Pattern noMeaningPattern = Pattern.compile("[a-zA-Z0-9]|\\s|μ|ξ|γ|π|р|у|к|н|в|ш|\\d*\\.|\\.\\d*|\\d{2}|\\d*%|\\d*\\.\\d*%|\\d*\\.\\d*");

        //String filePath = "data/distinctnewscontent.txt";
        //InputStream is = new FileInputStream(filePath);
        InputStream is = new FileInputStream(inputPath);
        String line; // 用来保存每行读取的内容
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        line = reader.readLine(); // 读取第一行
        while (line != null) { // 如果 line 为空说明读完了
            System.out.println(line);
            System.out.println("------------");
            List<Term> nlpParse = NlpAnalysis.parse(line);
            System.out.println(nlpParse);
            System.out.println("------------");

            for (Term term : nlpParse){
                if (!noMeaningWordList.contains(term.getNatureStr()) && !yaNoMeaningWordList.contains(term.getName()) && !noMeaningPattern.matcher(term.getName()).matches()){
                    System.out.println("name: " + term.getName());
                    System.out.println("nature: " + term.getNatureStr());
                    splitWordSet.add(term.getName());
                }else{
                    System.out.println("this is a no meaning word: " + term.getName());
                }
                //System.out.println("realname: " + term.getRealName());
            }

            line = reader.readLine(); // 读取下一行
        }
        reader.close();
        is.close();

        FileWriter writer = new FileWriter(new File(outputPath));
        int wordIndex = 0;
        String emptyWord = "";
        for (String word : splitWordSet){
            System.out.println("result word: " + word);
            /*if (throwEmptyWordAway){
                if (wordIndex == 76){
                    //System.out.println(word);
                    emptyWord = word;
                    writer.write(wordIndex + "\t" + word + "\n");
                }else{
                    writer.write(wordIndex + "\t" + word + "\n");
                }
                wordIndex += 1;
            }else {
                writer.write(wordIndex + "\t" + word + "\n");
                wordIndex += 1;
            }*/

            if (word.getBytes()[0] == 127){
                emptyWord = word;
            }else{
                writer.write(wordIndex + "\t" + word + "\n");
                wordIndex += 1;
            }

        }
        System.out.println(splitWordSet);
        System.out.println(splitWordSet.size());
        writer.flush();
        writer.close();
        //System.out.println("the only empty word: " + emptyWord.toCharArray());
        System.out.println("char array length: " + emptyWord.toCharArray().length);
        System.out.println("byte array length: " + emptyWord.getBytes().length);
        for (Character clea : emptyWord.toCharArray()){
            System.out.println(clea);
        }

        for (Byte clea : emptyWord.getBytes()){
            System.out.println(clea);
        }
    }

    public static void testRegex(){
        // 按指定模式在字符串查找
        String line = "This order was placed for QT3000! OK?";
        String pattern = "(.*)(\\d+)(.*)";

        // 创建 Pattern 对象
        Pattern r = Pattern.compile(pattern);

        // 现在创建 matcher 对象
        Matcher m = r.matcher(line);
        if (m.find( )) {
            System.out.println("Found value: " + m.group(0) );
            System.out.println("Found value: " + m.group(1) );
            System.out.println("Found value: " + m.group(2) );
        } else {
            System.out.println("NO MATCH");
        }

        //String line2 = "333阿斯达";
        //String line2 = "1088.";
        //String line2 = ".3";
        String line2 = "33.33";
        String patten2 = "[a-zA-Z0-9]|\\s|\\d*\\.|\\.\\d*|\\d{2}|\\d*%|\\d*\\.\\d*%|\\d*\\.\\d*";
        Pattern r2 = Pattern.compile(patten2);
        /*Matcher matcher = r2.matcher(line2);
        boolean b = matcher.matches();
        System.out.println(b);*/
        System.out.println(r2.matcher(line2).matches());
    }

    /*just for test*/
    public static void BasicTest(){
        List<Term> parse = BaseAnalysis.parse("让战士们过一个欢乐祥和的新春佳节。");
        System.out.println(parse);
        List<Term> yaParse = ToAnalysis.parse("让战士们过一个欢乐祥和的新春佳节。");
        System.out.println(yaParse);
        List<Term> nlpParse = NlpAnalysis.parse("洁面仪配合洁面深层清洁毛孔 清洁鼻孔面膜碎觉使劲挤才能出一点点皱纹 脸颊毛孔修复的看不见啦 草莓鼻历史遗留问题没辙 脸和脖子差不多颜色的皮肤才是健康的 长期使用安全健康的比同龄人显小五到十岁 28岁的妹子看看你们的鱼尾纹");
        System.out.println(nlpParse);
    }

    //yet another test
    public static void yaTest(){
        String[] noMeaningWords = {"x", "zg", "uj", "ul", "e", "d", "uz", "y", "w", "null"};
        List<String> noMeaningWordList = Arrays.asList(noMeaningWords);
        System.out.println(noMeaningWordList.contains("uj"));
        Set<String> splitWordSet = new HashSet<String>();

        String[] yaNoMeaningWords = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "%", "'", ";", ",", "-", ".", "/", ":", "?", ">", "<", "=", "@", "[", "]", "~", " ", "|", "ê", "é", "è", "á", "à"};
        List<String> yaNoMeaningWordList = Arrays.asList(yaNoMeaningWords);

        FilterModifWord.insertStopNatures("w") ;
        String newscontent = "有一个很美丽的姑娘，\"#!&'$%*+(让我很喜欢。3 2 1 333 cleantha ° ê é è á, 70多亿美元";
        List<Term> baseParse = BaseAnalysis.parse(newscontent);
        System.out.println(baseParse);
        List<Term> toParse = ToAnalysis.parse(newscontent);
        System.out.println(toParse);
        List<Term> nlpParse = NlpAnalysis.parse(newscontent);
        System.out.println(nlpParse);
        for (Term term : nlpParse){
            if (!noMeaningWordList.contains(term.getNatureStr()) && !yaNoMeaningWordList.contains(term.getName())){
                System.out.println("name: " + term.getName());
                System.out.println("nature: " + term.getNatureStr());
                splitWordSet.add(term.getName());
            }else{
                System.out.println("this is a no meaning word: " + term.getName());
            }
            //System.out.println("realname: " + term.getRealName());
        }

        System.out.println(splitWordSet);
        System.out.println(splitWordSet.size());
    }

    public static void main(String[] args) throws IOException{
        //yaTest();
        //这里生成的是新闻内容和新闻标题的词库，用来确定后面的词向量
        splitWordOnNewsContent("data/distinctnewscontent.txt", "data/splitnewscontent.txt", true);
        splitWordOnNewsContent("data/distinctnewstitle.txt", "data/splitnewstitle.txt", false);
        //testRegex();
    }
}
