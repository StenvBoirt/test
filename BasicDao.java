// Decompiled by Jad v1.5.8e. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.geocities.com/kpdus/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   BasicDAO.java

package com.bidesk.core.db.dao;

import com.bidesk.core.db.exception.DataAccessException;
import org.hibernate.Session;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;

public abstract class BasicDAO extends DAOCore {

    public BasicDAO() {
    }

    public Serializable insert(Object obj)
            throws DataAccessException {
        try {
            return super.insert(obj);
        } catch (Exception e) {
            throw new DataAccessException("insert failed!", e);
        }
    }

    public void update(Object obj)
            throws DataAccessException {
        try {
            super.update(obj);
        } catch (Exception e) {
            throw new DataAccessException("update failed!", e);
        }
    }

    public void update(String hql, Object values[])
            throws DataAccessException {
        try {
            super.update(hql, values);
        } catch (Exception e) {
            throw new DataAccessException("update failed!", e);
        }
    }

    public void delete(Object obj)
            throws DataAccessException {
        try {
            super.delete(obj);
        } catch (Exception e) {
            throw new DataAccessException("delete failed!", e);
        }
    }

    public void delete(Class c, Serializable s)
            throws DataAccessException {
        try {
            super.delete(c, s);
        } catch (Exception e) {
            throw new DataAccessException("delete failed!", e);
        }
    }

    public Object load(Class c, Serializable s)
            throws DataAccessException {
        try {
            return super.load(c, s);
        } catch (Exception e) {
            throw new DataAccessException("load failed!", e);
        }

    }

    public int getCount(String sql, List values)
            throws DataAccessException {
        Object obj;
        List list = null;
        try {
            list = super.createSQLQuery(sql, values, 0, 0);
        } catch (Exception e) {
            throw new DataAccessException("count failed!", e);
        }
        obj = list.get(0);
        if (obj == null)
            return 0;

        return Integer.parseInt(obj.toString());
    }

    public List createQuery(String hql, List values, int firstRow, int maxRows)
            throws DataAccessException {
        try {
            return super.createQuery(hql, values, firstRow, maxRows);
        } catch (Exception e) {
            throw new DataAccessException("hql query failed!", e);
        }

    }

    public List createSQLQuery(String sql, List values, int firstRow, int maxRows)
            throws DataAccessException {
        try {
            return super.createSQLQuery(sql, values, firstRow, maxRows);
        } catch (Exception e) {
            throw new DataAccessException("sql query failed!", e);
        }
    }

    public int execute(String sql, List values)
            throws DataAccessException {
        try {
            return super.execute(sql, values);
        } catch (Exception e) {
            throw new DataAccessException("sql execute failed!", e);
        }
    }

    /**
     * <p>输入表名、字段名以及字段长度则可获取生成的ID
     *
     * @param table
     * @param field
     * @param filedLen
     * @return String
     * author: liugw
     */
    public String generateId(String table, String field, int filedLen, String prefix) {
        StringBuilder ret = new StringBuilder();
        String sql = "";
        if (null != prefix && !"".equals(prefix))
            sql = "select max(to_number(substr(" + field + ", " + (prefix.length() + 1) + "))) from " + table + " where 1=1 order by " + field + " desc";
        else
            sql = "select max(to_number(" + field + ")) from " + table + " where 1=1 order by " + field + " desc";
        Object obj = createSQLQuery(sql, null, 0, 0).get(0);

        int prefixLen = 0;
        if (null != prefix && !"".equals(prefix)) {
            prefixLen = prefix.trim().length();
            /*if(null != obj && obj.toString().length()>prefixLen)
            {
    			obj = obj.toString().substring(prefixLen);
    		}*/
            ret.append(prefix.trim());
        }

        if (obj == null || "".equals(obj.toString()))
            obj = 0;

        String result = new BigInteger(obj.toString()).add(new BigInteger("1")).toString();
        int missLen = filedLen - result.length() - prefixLen;
        if (missLen < 0)
            return null;
        for (int i = 0; i < missLen; i++) {
            ret.append("0");
        }

        ret.append(result);

        return ret.toString();
    }

    /**
     * <p>输入表名、字段名以及字段长度、sql查询条件则可获取生成的ID
     *
     * @param table
     * @param field
     * @param filedLen
     * @return String
     * author: liugw
     */
    public String generateId(String table, String field, int filedLen, String prefix, String condition) {
        StringBuilder ret = new StringBuilder();
        String sql = "";
        if (null != prefix && !"".equals(prefix)) {
            sql = "select max(to_number(substr(" + field + ", " + (prefix.length() + 1) + "))) from " + table + " where 1=1  ";
        } else {
            sql = "select max(to_number(" + field + ")) from " + table + " where 1=1 ";
        }
        sql = sql + "and regexp_like(substr(user_id,11,4),'^\\d{4}$')";
        if (null != condition && !"".equalsIgnoreCase(condition))
            sql += condition;

        sql += " order by " + field + " desc";

        Object obj = createSQLQuery(sql, null, 0, 0).get(0);

        int prefixLen = 0;
        if (null != prefix && !"".equals(prefix)) {
            prefixLen = prefix.trim().length();
    		/*if(null != obj && obj.toString().length()>prefixLen)
    		{
    			obj = obj.toString().substring(prefixLen);
    		}*/
            ret.append(prefix.trim());
        }

        if (obj == null || "".equals(obj.toString()))
            obj = 0;

        String result = new BigInteger(obj.toString()).add(new BigInteger("1")).toString();
        int missLen = filedLen - result.length() - prefixLen;
        if (missLen < 0)
            return null;
        for (int i = 0; i < missLen; i++) {
            ret.append("0");
        }

        ret.append(result);

        return ret.toString();
    }

    /**
     * <p>输入表名、字段名 获取生成的ID
     *
     * @param table
     * @param field
     * @return String
     * author: liugw
     */
    public long generateId(String table, String field) {
        String sql = "select max(to_number(" + field + ")) from " + table + " where 1=1 order by " + field + " desc";
        Object obj = createSQLQuery(sql, null, 0, 0).get(0);

        if (obj == null || "".equals(obj.toString()))
            obj = 0;

        long result = Long.valueOf(obj.toString()) + 1;

        return result;
    }

    /**
     * <p>输入表名、字段名以及字段长度则可获取生成的ID,获取转让登记流水号
     *
     * @param table
     * @param field
     * @param filedLen
     * @return String
     */
    public String generateIdAssignNo(String table, String field, int filedLen, String prefix) {
        StringBuilder ret = new StringBuilder();
        String sqldate = "select to_char(sysdate,'yyyyMMdd') from dual";
        String todayDate = (String) createSQLQuery(sqldate, null, 0, 0).get(0);//取数据库时间

        String sql = "";
        sql = "select max(" + field + ") from " + table + " where 1=1 and substr(" + field + ",0,8)= '" + todayDate + "'";

        Object obj = null;
        List list = createSQLQuery(sql, null, 0, 0);
        if (list.size() > 0) {
            obj = list.get(0);
        }

        if (obj == null || "".equals(obj.toString()))
            obj = 0;

        String assignDate = "";
        //    	String todayDate = StrDateUtil.getCurentDateStr();//new SimpleDateFormat("yyyyMMdd").format(new Date());
        StringBuffer sb = new StringBuffer();
        sb.append(todayDate);
        if (obj.toString().length() > 8) {
            assignDate = obj.toString().substring(0, 8);
        }

        String result = "";
        if (assignDate.equals(todayDate)) {
            obj = obj.toString().substring(8);
            result = new BigInteger(obj.toString()).add(new BigInteger("1")).toString();
        } else {
            obj = 0;
            result = new BigInteger(obj.toString()).add(new BigInteger("1")).toString();
        }

        int missLen = filedLen - result.length() - 8;//8位日期长度
        //    	if(missLen<0){
        //    		return null;
        //    	}
        for (int i = 0; i < missLen; i++) {
            ret.append("0");
        }

        ret.append(result);

        return sb.append(ret.toString()).toString();
    }

    /**
     * <p>输入表名、字段名 获取生成的ID
     *
     * @param table
     * @param field
     * @return String
     * author: liugw
     */
    public String generateStrId(String table, String field) {

        String sql = "select max(to_number(" + field + ")) from " + table + " where 1=1 order by " + field + " desc";
        Object obj = createSQLQuery(sql, null, 0, 0).get(0);

        if (obj == null || "".equals(obj.toString()))
            obj = 0;

        long result = Long.valueOf(obj.toString()) + 1;

        return String.valueOf(result);
    }

    public void executeHql(String hql, List value) throws DataAccessException {
        super.executeHql(hql, value);
    }

}

