package org.apache.hadoop.hive.ql.parse;

import com.jyc.hive.SQLConstans;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.lib.Node;

import java.util.*;

/**
 * Created by jyc on 2018/7/16.
 */
public class SemanticAnalyzerEX extends SemanticAnalyzer {

    public static final String TBL_COL = "%s.%s";
    public static final String INSCLAUSE_0 = "insclause-0";

    public SemanticAnalyzerEX(HiveConf conf) throws SemanticException {
        super(conf);
    }

    public void genResolvedParseTree(ASTNode tree) throws SemanticException {
        genResolvedParseTree(tree, new PlannerContext());
    }

    /**
     * 通过Hive获取QueryBlock
     * @param sql
     * @return
     * @throws Exception
     */
    public static QB getQBFromSQL(String sql) throws Exception {
//        System.setProperty("hadoop.home.dir", "D:\\workspaces\\source codes\\hadoop");
//        System.setProperty("hadoop.home.dir", "classpath:*");

//        System.out.println("ok");

        HiveConf conf = new HiveConf();

        // 设置hive的session路径
        // new Context时会获取此路径存储session状态
        conf.set("_hive.local.session.path", "/tmp/local");
        conf.set("_hive.hdfs.session.path", "/tmp/hdfs");

        Context ctx = new Context(conf);

        SessionState ss = new SessionState(conf);
        ss.initTxnMgr(conf);
        SessionState.setCurrentSessionState(ss);


        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(sql, ctx);
        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }


        SemanticAnalyzerEX sem = new SemanticAnalyzerEX(conf);
        sem.init(true);
        sem.initCtx(ctx);

        // 解析出QB
        sem.genResolvedParseTree(tree);

        return sem.getQB();
    }

    public static void main(String[] args) {
        try {
//            QB qb = SemanticAnalyzerEX.getQBFromSQL(SQLConstans.SQL_05);
//            getBloodRelationShipFromSQL(SQLConstans.SQL_TEST);
            printResultInfo(getBloodRelationShipFromSQL(SQLConstans.SQL_01), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    static class BloodNode {
        String parentName;
        String name;
        Object data;
        boolean isSubQ;
        List<BloodNode> childrens = new ArrayList<>();

        public BloodNode(String parentName, String name, Object data, boolean isSubQ) {
            this.parentName = parentName;
            this.name = name;
            this.data = data;
            this.isSubQ = isSubQ;
        }
    }

    public static class Result {
        Table table ;
        ArrayList<Set<String>> resultList;
        int destType;

        Result(Table t, ArrayList<Set<String>> r, int d){
            this.table = t;
            this.resultList = r;
            this.destType = d;
        }
    }

    public static String printResultInfo(Result result, boolean onlyPrintTableVar){
        StringBuffer sb = new StringBuffer();

        int destType = result.destType;

        ArrayList<Set<String>> resultList = result.resultList;

        Table t = result.table;

        if (t == null){

            for (Set<String> strings : resultList) {
                if (onlyPrintTableVar){
                    strings.removeIf(s -> !s.startsWith("_table$$"));
                }
                sb.append(strings).append("\n");
                System.out.println(strings);

            }
            return sb.toString();
        }


        Map<String,Set<String>> finalResult = new LinkedHashMap<>();
        int index = 0;
        for (FieldSchema fs:t.getSd().getCols()) {
            finalResult.put((index+1)+":"+t.getDbName() + "." + t.getTableName() + "." + fs.getName(), resultList.get(index));
            index++;
        }

        if (destType == 1){
            for (FieldSchema fs:t.getPartitionKeys()){
                finalResult.put((index+1)+":"+t.getDbName() + "." + t.getTableName() + "." + fs.getName(), resultList.get(index));
                index++;
            }
        }


        for (Map.Entry<String, Set<String>> entry : finalResult.entrySet()) {
            Set<String> tableSet = new HashSet<>();
            for (String s : entry.getValue()) {
                if (onlyPrintTableVar){
                    if(s.startsWith("_table$$")){
                        tableSet.add(s);
                    }
                }else{
                    tableSet.add(s);
                }

            }
            String info = String.format("%s <==> %s", entry.getKey(), tableSet.toString());
            sb.append(info).append("\n");
            System.out.println(info);
        }

        return sb.toString();
    }

    public static Result getBloodRelationShipFromSQL(String sql) throws Exception {
        QB qb = getQBFromSQL(sql);

        BloodNode root = new BloodNode(null,"root", qb, false);

        buildChildrens(root);

        return analysis(root);
    }

    private static Result analysis(BloodNode root) {
        ArrayList<ArrayList<Map<String, String>>> analysisList = new ArrayList<>();
        internalAnalysis(root, analysisList);


        ArrayList<ArrayList<Map<String, String>>> notSureList = new ArrayList<>();
        ArrayList<Map<String, String>> sureList = new ArrayList<>();

        genSureAndNotSureList(analysisList, notSureList, sureList);

        matchNotSureList(notSureList, sureList);

        ArrayList<Map<String, String>> newSureList = new ArrayList<>();
        matchUnionTable(sureList, newSureList);

        rematchNotSureList(notSureList, newSureList);

        newSureList = distinctNewSureList(newSureList);

        ArrayList<Map<String, String>> outputList = analysisList.get(0);
        ArrayList<Map<String, String>> finalOutputList = handleOutputList(outputList, newSureList);


        ArrayList<Set<String>> resultList = new ArrayList<>();
        matchOutputCol(newSureList, finalOutputList, resultList);



        QB qb = (QB) root.data;

        int destType = qb.getMetaData().getDestTypeForAlias(INSCLAUSE_0);

        Table t = null;
        try {
            t = destType == 2? qb.getMetaData().getDestPartitionForAlias(INSCLAUSE_0).getTable().getTTable():qb.getMetaData().getDestTableForAlias(INSCLAUSE_0).getTTable();
        } catch (Exception e) {
            return new Result(null, resultList, destType);
        }

        return new Result(t, resultList, destType);
    }

    private static ArrayList<Map<String, String>> distinctNewSureList(ArrayList<Map<String, String>> newSureList) {
        Set<Map<String, String>> set = new LinkedHashSet();
        set.addAll(newSureList);
        return new ArrayList<>(set);
    }

    private static ArrayList<Map<String, String>> soutMappingInfoFromSureList(ArrayList<Map<String, String>> sureList, String mappingKey){
        ArrayList<Map<String, String>> mapList = new ArrayList<>();
        for (Map<String, String> map : sureList) {
            if (map.get("mappingInfoKey").equals(mappingKey)){
                System.out.println(map.get("mappingInfoVal"));
                mapList.add(map);
            }
        }
        return mapList;
    }



    private static ArrayList<Map<String, String>> handleOutputList(ArrayList<Map<String, String>> outputList, ArrayList<Map<String, String>> newSureList) {
        ArrayList<Map<String, String>> finalOutputList = new ArrayList<>();
        for (Map<String, String> outMap : outputList) {
            String mappingInfoKey = outMap.get("mappingInfoKey");
            if(mappingInfoKey.contains("*")){
                String matchKeyStr = mappingInfoKey.replace("*","");
                for (Map<String, String> sureMap : newSureList) {
                    if(sureMap.get("mappingInfoKey").startsWith(matchKeyStr)){
                        finalOutputList.add(sureMap);
                    }
                }
            }else{
                finalOutputList.add(getMapFromMappingListByKey(newSureList,mappingInfoKey));
            }
        }

        return finalOutputList;

    }

    private static void matchUnionTable(ArrayList<Map<String, String>> sureList, ArrayList<Map<String, String>> newSureList) {
        Map<String, Map<String, List<Map<String,String>>>> unionMap = new HashMap<>();
        for (Map<String, String> sureMap : sureList) {
            String mappingInfoKey = sureMap.get("mappingInfoKey");
            String mappingInfoVal = sureMap.get("mappingInfoVal").replaceAll("-subquery1\\.", "\\.").replaceAll("-subquery2\\.", "\\.");
            String[] tblAndCol = getTblAndCol(mappingInfoKey);
            String tblName = tblAndCol[0];
            String colName = tblAndCol[1];
            String[] subStrArray = new String[]{"-subquery1", "-subquery2"};
            for (int i = 0; i < subStrArray.length; i++) {
                if (tblName.endsWith(subStrArray[i])){
//                    tblName = tblName.replaceAll("-subquery1", "").replaceAll("-subquery2", "");
                    tblName = tblName.substring(0, tblName.lastIndexOf(subStrArray[i]));
//                    String finalTblName = handleTblName(tblName);
                    if (unionMap.containsKey(tblName)){
                        Map<String, List<Map<String,String>>> unionSubQMap = unionMap.get(tblName);
                        if (unionSubQMap == null){
                            unionSubQMap = new HashMap<>();
                            List<Map<String,String>> subqueryList = new ArrayList<>();
                            Map<String,String> subMap = new HashMap<>();
                            subMap.put("mappingInfoKey", tblName+"."+colName);
                            subMap.put("mappingInfoVal", mappingInfoVal);
                            subqueryList.add(subMap);
                            unionSubQMap.put("subquery_"+(i+1),subqueryList);
                        }else{

                            List<Map<String,String>> subqueryList = unionSubQMap.get("subquery_"+(i+1));
                            if (subqueryList == null){
                                subqueryList = new ArrayList<>();
                                Map<String,String> subMap = new HashMap<>();
                                subMap.put("mappingInfoKey", tblName+"."+colName);
                                subMap.put("mappingInfoVal", mappingInfoVal);
                                subqueryList.add(subMap);
                                unionSubQMap.put("subquery_"+(i+1),subqueryList);
                            }else{
                                if(!ifMappingListContainsMap(subqueryList,tblName+"."+colName)){
                                    Map<String,String> subMap = new HashMap<>();
                                    subMap.put("mappingInfoKey", tblName+"."+colName);
                                    subMap.put("mappingInfoVal", mappingInfoVal);

                                    subqueryList.add(subMap);
                                }

                            }

                        }
                    }else{

                        Map<String, List<Map<String,String>>> unionSubQMap = new HashMap<>();
                        List<Map<String,String>> subqueryList = new ArrayList<>();
                        Map<String,String> subMap = new HashMap<>();
                        subMap.put("mappingInfoKey", tblName+"."+colName);
                        subMap.put("mappingInfoVal", mappingInfoVal);
                        subqueryList.add(subMap);
                        unionSubQMap.put("subquery_"+(i+1) , subqueryList);
                        unionMap.put(tblName, unionSubQMap);
                    }
                }
            }

        }


        // 合并union相同位置的字段, 字段名以subquery1为主
        Map<String,Map<String,String>> unionFinalMap = new HashMap<>();
        buildFinalUnionMap(unionMap, unionFinalMap);


        /**
         * 重组sureList
         * 1. subquery2结尾的不要
         * 2. subquery1结尾的将合并后的加入到新的sureList
         * 3. 不是1和2的,替换掉全部subquery字符串,加入到新sureList
         */
        for (Map<String, String> sureMap : sureList) {
            String mappingInfoKey = sureMap.get("mappingInfoKey");
            String mappingInfoVal = sureMap.get("mappingInfoVal");
            String[] tblAndCol = getTblAndCol(mappingInfoKey);
            String tblName = tblAndCol[0];
//            String[] valTblAndCol = getTblAndCol(mappingInfoVal);
//            String valTblName = valTblAndCol[0];


            if (tblName.endsWith("-subquery2") || tblName.matches("(.*)subquery(1|2)-subquery(1|2)$")){
//            if (tblName.endsWith("-subquery2") || mappingInfoVal.contains("-subquery2")){
                continue;
            }else{
                mappingInfoKey = mappingInfoKey.replaceAll("-subquery1\\.", "\\.");
                mappingInfoVal = mappingInfoVal.replaceAll("-subquery1\\.", "\\.").replaceAll("-subquery2\\.", "\\.");
                if (unionFinalMap.containsKey(mappingInfoKey)){
                    newSureList.add(unionFinalMap.get(mappingInfoKey));
                }else {
                    Map<String,String> map = new HashMap<>();
                    map.put("mappingInfoKey", mappingInfoKey);
                    map.put("mappingInfoVal", mappingInfoVal);
                    newSureList.add(map);
                }
            }

        }

        for (Map.Entry<String, Map<String, String>> entry : unionFinalMap.entrySet()) {
            if (!ifMappingListContainsMap(newSureList, entry.getKey())){
                newSureList.add(entry.getValue());
            }
        }

        return ;

    }

    /**
     * 如果是连续union的,需要将tblname中的subquery处理掉,得到最终的真实表名
     * @param tblName
     * @return
     */
    private static String handleTblName(String tblName) {
        while (tblName.endsWith("-subquery1") || tblName.endsWith("-subquery2") ){
            tblName = tblName.substring(0, tblName.lastIndexOf("-subquery"));
        }
        return tblName;
    }

    private static void buildFinalUnionMap(Map<String, Map<String, List<Map<String, String>>>> unionMap, Map<String, Map<String, String>> unionFinalMap) {

        List<String> sortList = new ArrayList(unionMap.keySet());
        Collections.sort(sortList, (o1, o2) -> -o1.compareTo(o2));


        for (String key : sortList) {
            String tblName = key;
            Map<String, List<Map<String, String>>> subqMap = unionMap.get(key);

            List<Map<String, String>> subquery1List = subqMap.get("subquery_1");
            List<Map<String, String>> subquery2List = subqMap.get("subquery_2");
            List<Map<String, String>> subqueryUnionList = new ArrayList<>();

            for (int i = 0; i < subquery1List.size(); i++) {
                String mappingInfoKey = subquery1List.get(i).get("mappingInfoKey");
                String mappingInfoVal = null;

                if (isSubQueryTblName(tblName)){
                    mappingInfoKey = substringSqubqueryTblName(getTblAndCol(mappingInfoKey)[0]) + "." + getTblAndCol(mappingInfoKey)[1];
                }

                try {
                    mappingInfoVal = StringUtils.join(subquery1List.get(i).get("mappingInfoVal"),",",subquery2List.get(i).get("mappingInfoVal"));
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                Map<String,String> map = new HashMap<>();
                map.put("mappingInfoKey", mappingInfoKey);
                map.put("mappingInfoVal", mappingInfoVal);
                subqueryUnionList.add(map);

                if (!isSubQueryTblName(tblName)){
                    unionFinalMap.put(mappingInfoKey, map);
                }
            }

            if (isSubQueryTblName(tblName)){
                tblName = substringSqubqueryTblName(tblName);
                unionMap.get(tblName).put("subquery_1", subqueryUnionList);
            }
            
        }
    }

    private static boolean isSubQueryTblName(String tblName) {
        return tblName.endsWith("-subquery1") || tblName.endsWith("-subquery2");
    }
    
    private static String substringSqubqueryTblName(String tblName){
        return tblName.substring(0,tblName.lastIndexOf("-subquery"));
    }

    private static String[] getTblAndCol(String str) {
        int dotIndex = str.lastIndexOf(".");
        String tblName = str.substring(0, dotIndex);
        String colName = str.substring(dotIndex+1);
        return new String[]{tblName, colName};
    }

    private static void matchOutputCol(ArrayList<Map<String, String>> sureList, ArrayList<Map<String, String>> outputList, ArrayList<Set<String>> resultList) {
        for (Map<String, String> outMap : outputList) {
            Set<String> matchSet = new TreeSet<>();
//            Map<String, String> suMap = getMapFromMappingListByKey(sureList, outMap.get("mappingInfoKey"));
            matchOutMap(outMap, matchSet, sureList);
            resultList.add(matchSet);
        }
    }

    private static void matchOutMap(Map<String, String> outMap, Set<String> matchSet, ArrayList<Map<String, String>> sureList) {
        String mappingInfoVal = outMap.get("mappingInfoVal");
        String[] mappingInfoValArray = mappingInfoVal.split(",");
        for (String str : mappingInfoValArray) {
            if (str.startsWith("_table$$") || str.startsWith("_constant$$")){
                matchSet.add(str);
            }else{
                for (Map<String, String> sureMap : sureList) {
                    if (sureMap.get("mappingInfoKey").equals(str)){
                        matchOutMap(sureMap, matchSet, sureList);
//                        break ;
                    }
                }
            }
        }



    }

    private static void matchNotSureList(ArrayList<ArrayList<Map<String, String>>> notSureList, ArrayList<Map<String, String>> sureList) {
        Collections.reverse(notSureList);


        for (ArrayList<Map<String, String>> element : notSureList) {
            for (Map<String, String> map : element) {
                String mappingInfoKey = map.get("mappingInfoKey");
                String mappingInfoVal = map.get("mappingInfoVal");
                String index = map.get("index");

                if(mappingInfoKey.contains("*")){
                    String[] notSureStrArray = mappingInfoVal.split(",");
                    int secondIndex = 0;
                    for (int i = 0; i < notSureStrArray.length; i++) {
                        String notSureMappingInfoKey = notSureStrArray[i];
                        String matchStr = notSureMappingInfoKey.replace("*","");
                        ArrayList<Map<String, String>> matchList = new ArrayList<>();
                        for (Map<String, String> sureMap : sureList) {
                            String sureMappingInfoKey = sureMap.get("mappingInfoKey");
                            if(sureMappingInfoKey.startsWith(matchStr)){
                                String sureMappingInfoKeyCol = sureMappingInfoKey.replace(matchStr,"");
                                String newSureMappingInfoKey = notSureMappingInfoKey.substring(0, notSureMappingInfoKey.lastIndexOf("^")) + "." + sureMappingInfoKeyCol;
                                Map<String,String> m = new HashMap<>();
                                m.put("mappingInfoKey", newSureMappingInfoKey);
                                m.put("mappingInfoVal", sureMappingInfoKey);
                                m.put("isSure", "true");
                                m.put("index", index+"_"+secondIndex);
                                matchList.add(m);
                                secondIndex ++;
                            }
                        }
                        sureList.addAll(matchList);
                    }
                }else{
                    ArrayList<String> matchedColList = new ArrayList<>();
                    String[] matchColArray = mappingInfoVal.split(",");
                    for (String matchCol : matchColArray) {
                        String[] matchStrArray = matchCol.split("\\|\\|");
                        OK:
                        for (String matchStr : matchStrArray) {
                            if (matchStr.startsWith("_table$$") || matchStr.startsWith("_constant$$")){
                                matchedColList.add(matchStr);
                                continue ;
                            }

                            for (Map<String, String> sureMap : sureList) {
                                String sureMappingInfoKey = sureMap.get("mappingInfoKey");
                                if(sureMappingInfoKey.equals(matchStr)){
                                    matchedColList.add(sureMappingInfoKey);
                                    break OK;
                                }
                            }
                        }
                    }
                    Map<String,String> m = new HashMap<>();
                    m.put("mappingInfoKey", mappingInfoKey);
                    m.put("mappingInfoVal", StringUtils.join(matchedColList, ","));
                    m.put("isSure", "true");
                    m.put("index", index);
//                    matchList.add(m);
                    sureList.add(m);

                }


            }
        }

        // 重新排序surelist
        Collections.sort(sureList, new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> o1, Map<String, String> o2) {
                String[] index1StrArray = o1.get("index").split("_");
                String[] index2StrArray = o2.get("index").split("_");
                if (Integer.valueOf(index1StrArray[0]).compareTo(Integer.valueOf(index2StrArray[0])) == 0){
                    return Integer.valueOf(index1StrArray[1]).compareTo(Integer.valueOf(index2StrArray[1]));
                }else{
                    return Integer.valueOf(index1StrArray[0]).compareTo(Integer.valueOf(index2StrArray[0]));
                }
            }
        });
//        sureList.addAll(matchList);
    }

    private static void rematchNotSureList(ArrayList<ArrayList<Map<String, String>>> notSureList, ArrayList<Map<String, String>> sureList) {

        for (ArrayList<Map<String, String>> element : notSureList) {
            for (Map<String, String> map : element) {
                String mappingInfoKey = map.get("mappingInfoKey");
                String mappingInfoVal = map.get("mappingInfoVal");

                if(mappingInfoKey.contains("*")){
                    String[] notSureStrArray = mappingInfoVal.split(",");
                    for (int i = 0; i < notSureStrArray.length; i++) {
                        String notSureMappingInfoKey = notSureStrArray[i];
                        String matchStr = notSureMappingInfoKey.replace("*","");
                        ArrayList<Map<String, String>> matchList = new ArrayList<>();
                        for (Map<String, String> sureMap : sureList) {
                            String sureMappingInfoKey = sureMap.get("mappingInfoKey");
                            if(sureMappingInfoKey.startsWith(matchStr)){
                                String sureMappingInfoKeyCol = sureMappingInfoKey.replace(matchStr,"");
                                String newSureMappingInfoKey = notSureMappingInfoKey.substring(0, notSureMappingInfoKey.lastIndexOf("^")) + "." + sureMappingInfoKeyCol;
                                Map<String,String> m = new HashMap<>();
                                m.put("mappingInfoKey", newSureMappingInfoKey);
                                m.put("mappingInfoVal", sureMappingInfoKey);
                                matchList.add(m);
                            }
                        }
                        sureList.addAll(matchList);
                    }
                }else{
                    ArrayList<String> matchedColList = new ArrayList<>();
                    String[] matchColArray = mappingInfoVal.split(",");
                    for (String matchCol : matchColArray) {
                        String[] matchStrArray = matchCol.split("\\|\\|");
                        OK:
                        for (String matchStr : matchStrArray) {
                            if (matchStr.startsWith("_table$$") || matchStr.startsWith("_constant$$")){
                                matchedColList.add(matchStr);
                                continue ;
                            }

                            for (Map<String, String> sureMap : sureList) {
                                String sureMappingInfoKey = sureMap.get("mappingInfoKey");
                                if(sureMappingInfoKey.equals(matchStr)){
                                    matchedColList.add(sureMappingInfoKey);
                                    break OK;
                                }
                            }
                        }
                    }
                    Map<String,String> m = new HashMap<>();
                    m.put("mappingInfoKey", mappingInfoKey);
                    m.put("mappingInfoVal", StringUtils.join(matchedColList, ","));
                    sureList.add(m);

                }


            }
        }

    }

    private static void genSureAndNotSureList(ArrayList<ArrayList<Map<String, String>>> analysisList, ArrayList<ArrayList<Map<String, String>>> notSureList, ArrayList<Map<String, String>> sureList) {

        int index = 0;
        for (ArrayList<Map<String, String>> element : analysisList) {
            ArrayList<Map<String, String>> notSureInnerList = new ArrayList<>();
            for (Map<String, String> map : element) {
                map.put("index", index+"");
                if(Boolean.valueOf(map.get("isSure"))){
                    sureList.add(map);
                }else{
                    notSureInnerList.add(map);
                }
                index++;
            }
            notSureList.add(notSureInnerList);
        }
    }

    private static void internalAnalysis(BloodNode node, ArrayList<ArrayList<Map<String, String>>> analysisList) {

        if (!(node.data instanceof QBExpr)){
            ArrayList<Map<String, String>> colMappingList = getColMapping(node);
            analysisList.add(colMappingList);
        }

        for (BloodNode child:node.childrens) {
            internalAnalysis(child, analysisList);
        }

    }

    private static ArrayList<Map<String, String>> getColMapping(BloodNode bloodNode) {
        ArrayList<Map<String,String>> totalColMappingList = new ArrayList<>();

        if (bloodNode.data instanceof QB){
            QB q = (QB) bloodNode.data;
            ASTNode selectNode = q.getParseInfo().getSelForClause(INSCLAUSE_0);

            for (int i = 0; i < selectNode.getChildCount(); i++) {
                ASTNode astNode = (ASTNode) selectNode.getChild(i);
                ArrayList<Map<String,String>> colMappingList = new ArrayList<>();
                if (astNode.getChildren().size() == 2){
                    String colAlias = astNode.getChild(1).getText();
                    String targetName = String.format(TBL_COL,bloodNode.name,colAlias);
                    handleAstNodeToColMappingList(colMappingList, targetName, astNode.getChild(0), bloodNode, i);
                }else{
                    handleAstNodeToColMappingList(colMappingList, null, astNode.getChild(0), bloodNode, i);
                }

                if(colMappingList.size() == 1){
                    totalColMappingList.add(colMappingList.get(0));
                }else if (colMappingList.size() > 1){
                    String hasNotSureCol = null;
                    Map<String,String> mappingInfoMap = new HashMap<>();
                    Set<String> filterSet = new HashSet<>();
                    for (Map<String,String> map:colMappingList) {
                        if (hasNotSureCol == null && !Boolean.valueOf(map.get("isSure"))){
                            hasNotSureCol = "true";
                        }

                        String mappingInfoKey = map.get("mappingInfoKey");
                        String mappingInfoVal = map.get("mappingInfoVal");
                        mappingInfoVal = mappingInfoVal==null ? "":mappingInfoVal;

                        if(!mappingInfoMap.containsKey("mappingInfoKey")){
                            mappingInfoMap.put("mappingInfoKey", mappingInfoKey);
                            mappingInfoMap.put("mappingInfoVal", mappingInfoVal);
                        }else{
                            if (mappingInfoMap.get("mappingInfoKey").equals(mappingInfoKey) && !filterSet.contains(mappingInfoVal)){
                                mappingInfoMap.put("mappingInfoVal", mappingInfoMap.get("mappingInfoVal") + "," + mappingInfoVal);
                            }
                        }
                        filterSet.add(mappingInfoVal);
                    }
                    if(Boolean.valueOf(hasNotSureCol)){
                        mappingInfoMap.put("isSure", "false");
                    }else{
                        mappingInfoMap.put("isSure", "true");
                    }

                    totalColMappingList.add(mappingInfoMap);
                }
            }
        }else{
            Table t = (Table) bloodNode.data;
            for (FieldSchema fs:t.getSd().getCols()) {
                handleMetaTableCol(bloodNode, totalColMappingList, t, fs);
            }

            for (FieldSchema fs:t.getPartitionKeys()){
                handleMetaTableCol(bloodNode, totalColMappingList, t, fs);
            }
        }

        return totalColMappingList;
    }

    private static void handleMetaTableCol(BloodNode bloodNode, ArrayList<Map<String, String>> totalColMappingList, Table t, FieldSchema fs) {
        String mappingInfoKey = String.format(TBL_COL, bloodNode.name,fs.getName());
        String mappingInfoVal = String.format("_table$$%s.%s.%s", t.getDbName(),t.getTableName(),fs.getName());
        Map<String,String> colmap = new HashMap<>();
        colmap.put("mappingInfoKey", mappingInfoKey.toLowerCase());
        colmap.put("mappingInfoVal", mappingInfoVal.toLowerCase());
        colmap.put("isSure", "true");
        totalColMappingList.add(colmap);
    }

    private static void handleAstNodeToColMappingList(ArrayList<Map<String, String>> colMappingList, String targetName, Tree node, BloodNode bloodNode, int i) {

        String isSure = "false";
        String mappingInfoKey = null;
        String mappingInfoVal = null;
        String col;

        if (".".equals(node.getText())){
            String table = node.getChild(0).getChild(0).getText();
            if (node.getChild(1).getType() == HiveParser.TOK_ALLCOLREF){
                col = "*";
                mappingInfoKey = String.format(TBL_COL, bloodNode.name, col);
                mappingInfoVal = String.format(TBL_COL, buildName(bloodNode.name, table), col);
            }
            else {
                col = node.getChild(1).getText();
                isSure = "true";

                if (targetName != null){
                    mappingInfoKey = targetName;
                }else{
                    mappingInfoKey = String.format(TBL_COL, bloodNode.name, col);
                }

                mappingInfoVal = String.format(TBL_COL, buildName(bloodNode.name, table), col);

            }
        }
        else if (node.getType() == HiveParser.TOK_TABLE_OR_COL){
            col = node.getChild(0).getText();
            ArrayList<String> mappingValueList = new ArrayList<>();
            for (BloodNode child:bloodNode.childrens) {
                mappingValueList.add(String.format(TBL_COL,child.name,col));
            }

            if(mappingValueList.size() == 1){
                isSure = "true";
            }

            if (targetName != null){
                mappingInfoKey = targetName;
            }else{
                mappingInfoKey = String.format(TBL_COL, bloodNode.name, col);
            }
            mappingInfoVal = StringUtils.join(mappingValueList, "||");
        }
        else if (node.getType() == HiveParser.TOK_ALLCOLREF || node.getType() == HiveParser.TOK_FUNCTIONSTAR){
            col = "*";
            ArrayList<String> mappingValueList = new ArrayList<>();
            for (BloodNode child:bloodNode.childrens) {
                mappingValueList.add(String.format(TBL_COL,child.name,col));
            }
            mappingInfoKey = String.format(TBL_COL, bloodNode.name, col);
            mappingInfoVal = StringUtils.join(mappingValueList, ",");
        }
        else if (node.getType() == HiveParser.TOK_FUNCTION){
            ASTNode astNode = (ASTNode)node;
            for (Node ast:astNode.getChildren()) {
                targetName = (targetName == null ? String.format(TBL_COL, bloodNode.name, "_col" + i) : targetName);
                handleAstNodeToColMappingList(colMappingList, targetName, (Tree) ast, bloodNode, i);
            }
            return;
        }
        else if (node.getType() == HiveParser.TOK_NULL ||
                node.getType() == HiveParser.StringLiteral ||
                node.getType() == HiveParser.Number ||
                node.getType() == HiveParser.TOK_ISNOTNULL ||
                node.getType() == HiveParser.TOK_ISNULL ){

            isSure = "true";
            if (targetName != null){
                mappingInfoKey = targetName;
            }else{
                mappingInfoKey = String.format(TBL_COL, bloodNode.name, "_col" + i);
            }
            col = node.getText();
            mappingInfoVal = "_constant$$" + col;

        }


        if(mappingInfoKey != null){

            /*if (ifMappingListContainsMap(colMappingList, mappingInfoKey)){
                Map<String,String> colMappings = getMapFromMappingListByKey(colMappingList, mappingInfoKey);
                String mappingInfoV = colMappings.get("mappingInfoVal");
                if (mappingInfoVal != null){
                    mappingInfoV = mappingInfoKey + "," + mappingInfoVal;
                }
                colMappings.put("mappingInfoVal",mappingInfoV.toLowerCase());
            }*/

            Map<String,String> colMappings = new HashMap<>();
            colMappings.put("mappingInfoKey",mappingInfoKey.toLowerCase());
            colMappings.put("mappingInfoVal",String.valueOf(mappingInfoVal).toLowerCase());
            colMappings.put("isSure",isSure);
            colMappingList.add(colMappings);
        }else{
            ASTNode astNode = (ASTNode)node;
            if (astNode.getChildren() == null){
                return;
            }
            for (Node ast:astNode.getChildren()) {
                targetName = (targetName == null ? String.format(TBL_COL, bloodNode.name, "_col" + i) : targetName);
                handleAstNodeToColMappingList(colMappingList, targetName, (Tree) ast, bloodNode, i);
            }
        }



    }

    private static Map<String,String> getMapFromMappingListByKey(ArrayList<Map<String, String>> colMappingList, String mappingInfoKey) {
        for (Map<String, String> map : colMappingList) {
            if (map.get("mappingInfoKey").equals(mappingInfoKey)){
                return map;
            }
        }
        return null;
    }

    private static boolean ifMappingListContainsMap(List<Map<String, String>> colMappingList, String mappingInfoKey) {
        for (Map<String, String> map : colMappingList) {
            if (map.get("mappingInfoKey").equals(mappingInfoKey)){
                return true;
            }
        }
        return false;
    }

    private static String buildName(String parentName,String name){
        return StringUtils.join(parentName,"^",name);
    }


    private static void buildChildrens(BloodNode bloodNode) {
        Object data = bloodNode.data;

        if (data instanceof QB){
            QB qb = (QB) data;
            BloodNode children;
            for (String alias:qb.getAliases()) {
                if (qb.getTabAliases().contains(alias)){
                    Table t = qb.getMetaData().getAliasToTable().get(alias).getTTable();
                    children = new BloodNode(bloodNode.name,buildName(bloodNode.name,alias),t, false);
                    bloodNode.childrens.add(children);
                }
                else if (qb.getSubqAliases().contains(alias)){
                    if(qb.getSubqForAlias(alias).getOpcode().name().equals("UNION")){
                        children = new BloodNode(bloodNode.name,buildName(bloodNode.name,alias),qb.getSubqForAlias(alias), true);
                        bloodNode.childrens.add(children);
                    }
                    else if (qb.getSubqForAlias(alias).getOpcode().name().equals("NULLOP")){
                        QB q = qb.getSubqForAlias(alias).getQB();
                        children = new BloodNode(bloodNode.name,buildName(bloodNode.name,alias),q, true);
                        bloodNode.childrens.add(children);
                    }
                }

            }


        }
        else if (data instanceof QBExpr){
            QBExpr qbe = (QBExpr)data;
            QBExpr qbe1 = qbe.getQBExpr1();
            String alias = qbe1.getAlias();
            String subStr = alias.substring(alias.lastIndexOf("-"));
            BloodNode children = new BloodNode(bloodNode.name,bloodNode.name+subStr,qbe1.getQB()==null?qbe1:qbe1.getQB(), true);
            bloodNode.childrens.add(children);

            QBExpr qbe2 = qbe.getQBExpr2();
            alias = qbe2.getAlias();
            subStr = alias.substring(alias.lastIndexOf("-"));
            children = new BloodNode(bloodNode.name,bloodNode.name+subStr,qbe2.getQB()==null?qbe2:qbe2.getQB(), true);
            bloodNode.childrens.add(children);
        }
        else if (data instanceof Table){
            return;
        }

        if (!bloodNode.childrens.isEmpty()){
            for (BloodNode n: bloodNode.childrens) {
                buildChildrens(n);
            }
        }
    }

}
