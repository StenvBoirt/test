package com.bidesk.core.db.dao;

import org.apache.commons.lang3.math.NumberUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.transform.Transformers;

import javax.annotation.Resource;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.*;

/**
 * 数据库基础操作类
 * <p/>
 * Created by zhangxin on 2015-07-27.
 */
public class BaseDao<T> implements IBaseDao<T> {

    @Resource
    private SessionFactory sessionFactory;

    private Class<T> entityClass;

    /**
     * 构造器
     */
    public BaseDao() {
        //利用泛型的反射获取实体类
        entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * 获取与当前线程绑定的 SESSION
     *
     * @return
     */
    private Session getCurrentSession() {
        return sessionFactory.getCurrentSession();
    }

    /**
     * 获取主键名
     *
     * @return
     */
    private Serializable getPkColumnName() {
        return getCurrentSession().getSessionFactory().getClassMetadata(entityClass).getIdentifierPropertyName();
    }

    /**
     * 根据主键获取对象(延迟加载)
     *
     * @param id：主键
     * @return：代理对象
     */
    public T load(Serializable id) {
        return (T) getCurrentSession().load(entityClass, id);
    }

    /**
     * 根据主键获取对象
     *
     * @param id：主键
     * @return：对象
     */
    public T get(Serializable id) {
        return (T) getCurrentSession().get(entityClass, id);
    }

    /**
     * 获取所有对象，并根据主键作升序排序
     *
     * @return：对象集合
     */
    public List<T> getAll() {
        return getCurrentSession().createQuery("FROM " + entityClass.getSimpleName() + " ORDER BY " + getPkColumnName() + " ASC ").list();
    }

    /**
     * 取所有对象，并根据主键作升序排序
     * 迭代器，适用于数据量较大的情况
     *
     * @return：代理对象集合
     */
    public Iterator<T> getAllDelay() {
        return getCurrentSession().createQuery("FROM " + entityClass.getSimpleName() + " ORDER BY " + getPkColumnName() + " ASC ").iterate();
    }

    /**
     * 保存对象
     *
     * @param o：对象
     * @return：主键
     */
    public BigDecimal save(Object o) {
        return new BigDecimal(getCurrentSession().save(o).toString());
    }

    /**
     * 更新对象
     *
     * @param o：对象
     */
    public void update(Object o) {
        getCurrentSession().update(o);
    }

    /**
     * 更新操作（HQL）
     *
     * @param hql：HQL语句
     * @param params：变参数组
     * @return：更新记录数
     */
    public int update(String hql, Object... params) {
        return executeHQLUpdate(hql, params);
    }

    /**
     * 更新操作（HQL）
     *
     * @param hql：HQL语句
     * @param paramMap：Map类型参数
     * @return：更新记录数
     */
    public int update(String hql, Map<String, Object> paramMap) {
        return executeHQLUpdate(hql, paramMap);
    }

    /**
     * 更新操作（SQL）
     *
     * @param sql：SQL语句
     * @param params：变参数组
     * @return：更新记录数
     */
    public int updateBySQL(String sql, Object... params) {
        return executeSQLUpdate(sql, params);
    }

    /**
     * 更新操作（SQL）
     *
     * @param sql：SQL语句
     * @param paramMap：Map类型参数
     * @return：更新记录数
     */
    public int updateBySQL(String sql, Map<String, Object> paramMap) {
        return executeSQLUpdate(sql, paramMap);
    }

    /**
     * 根据 HQL 语句查询所有结果
     * 适用于数据量不大的情况
     *
     * @param hql：HQL语句
     * @return：对象结果集
     */
    public List<T> find(String hql) {
        return getCurrentSession().createQuery(hql).list();
    }


    /**
     * 根据 HQL 语句查询所有结果
     * 适用于数据量较大的情况
     *
     * @param hql
     * @return：代理对象集合
     */
    public Iterator<T> findDelay(String hql) {
        return getCurrentSession().createQuery(hql).iterate();
    }

    /**
     * 根据 HQL 和 param 查询所有结果
     *
     * @param hql
     * @param params：变参数组
     * @return：对象结果集
     */
    public List<T> find(String hql, Object... params) {
        Query query = getCurrentSession().createQuery(hql);
        return getFinalQuery(query, params).list();
    }

    /**
     * 根据 HQL 和 param 查询所有结果
     *
     * @param hql
     * @param paramMap：Map类型参数
     * @return：对象结果集
     */
    public List<T> find(String hql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createQuery(hql);
        return getFinalQuery(query, paramMap).list();
    }


    /**
     * 根据 HQL 语句查询所有结果
     * 适用于数据量不大的情况
     *
     * @param hql：HQL语句
     * @return：对象结果集
     */
    public T findUnique(String hql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createQuery(hql);
        List<T> list = getFinalQuery(query, paramMap).list();
        return list != null && list.size() > 0 ? list.get(0) : null;
    }

    /**
     * 根据 SQL 和 param 查询所有结果
     *
     * @param sql：SQL语句
     * @param params：变参数组
     * @return：结果集
     */
    public List<Map<String, Object>> findBySQL(String sql, Object... params) {
        Query query = getCurrentSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP);
        List<Map<String, Object>> list = getFinalQuery(query, params).list();
        return keyToLowerCase(list);
    }

    /**
     * 根据 SQL 和 param 查询所有结果
     *
     * @param sql：SQL语句
     * @param paramMap：Map类型参数
     * @return：结果集
     */
/*    public List<String> findBySQLList(String sql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createSQLQuery(sql).setResultTransformer(Transformers.TO_LIST);
        return getFinalQuery(query, paramMap).list();
    }*/

    /**
     * 根据 SQL 和 param 查询所有结果
     *
     * @param sql：SQL语句
     * @param paramMap：Map类型参数
     * @return：结果集
     */
    public List findBySQLList(String sql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createSQLQuery(sql).setResultTransformer(Transformers.TO_LIST);
        return getFinalQuery(query, paramMap).list();
    }

    /**
     * 根据 SQL 和 param 查询所有结果
     *
     * @param sql：SQL语句
     * @param paramMap：Map类型参数
     * @return：结果集
     */
    public List<Map<String, Object>> findBySQL(String sql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP);
        List<Map<String, Object>> list = getFinalQuery(query, paramMap).list();
        return keyToLowerCase(list);
    }

    /**
     * 根据 SQL 和 param 查询所有结果
     *
     * @param sql ：SQL语句
     * @param paramMap ：Map类型参数
     * @return：唯一结果
     */
    public Map<String, Object> findBySQLUnique(String sql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createSQLQuery(sql).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP);
        List<Map<String, Object>> list = getFinalQuery(query, paramMap).list();
        return keyToLowerCase(list).get(0);
    }

    /**
     * 分页查询数据(HQL)
     *
     * @param hql：HQL语句
     * @param paramMap：Map类型参数
     * @param pageNum：当前页码
     * @param pageSize：每页记录数
     * @return：Map(k, v)(pageNum：当前页码；pageSize：每页记录数；totalCount：总记录数；result：结果集)
     */
    public Map<String, Object> find(String hql, Map<String, Object> paramMap, int pageNum, int pageSize) {
        Query query = getCurrentSession().createQuery(hql);
        query.setFirstResult((pageNum - 1) * pageSize).setMaxResults(pageSize);
        List list = getFinalQuery(query, paramMap).list();

        Map<String, Object> paginationMap = new HashMap<String, Object>();
        paginationMap.put("pageNum", pageNum);//当前页码
        paginationMap.put("pageSize", pageSize);//每页记录数
        //查询总记录数
        BigDecimal totalCount = count(hql, paramMap);
        paginationMap.put("totalCount", totalCount);//总记录数
        //计算总页数，“不保留小数”并且做“进位处理”
        BigDecimal totalPage = totalCount.divide(new BigDecimal(pageSize), NumberUtils.BYTE_ZERO, BigDecimal.ROUND_UP);
        paginationMap.put("totalPage", totalPage);//总页数
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("pagination", paginationMap);

        resultMap.put("list", list);//结果集
        return resultMap;

    }

    /**
     * 获取总记录数（HQL）
     *
     * @param hql：HQL语句
     * @param paramMap：Map参数
     * @return：统计结果
     */
    private BigDecimal count(String hql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createQuery("SELECT COUNT(1) " + hql);
        return new BigDecimal(getFinalQuery(query, paramMap).uniqueResult().toString());
    }

    /**
     * 分页查询数据(SQL)
     *
     * @param sql：SQL语句
     * @param paramMap：Map类型参数
     * @param pageNum：当前页码
     * @param pageSize：每页记录数
     * @return：Map(k, v)(pageNum：当前页码；pageSize：每页记录数；totalCount：总记录数；result：结果集)
     */
    public Map<String, Object> findBySQL(String sql, Map<String, Object> paramMap, int pageNum, int pageSize) {
        Query query = getCurrentSession().createSQLQuery(sql);
        query.setFirstResult((pageNum - 1) * pageSize).setMaxResults(pageSize);
        query.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP);//设定结果集中的每个对象为Map类型
        List<Map<String, Object>> list = getFinalQuery(query, paramMap).list();

        Map<String, Object> paginationMap = new HashMap<String, Object>();
        paginationMap.put("pageNum", pageNum);//当前页码
        paginationMap.put("pageSize", pageSize);//每页记录数
        //查询总记录数
        BigDecimal totalCount = countBySQL(sql, paramMap);
        paginationMap.put("totalCount", totalCount);//总记录数
        //计算总页数，“不保留小数”并且做“进位处理”
        BigDecimal totalPage = totalCount.divide(new BigDecimal(pageSize), NumberUtils.BYTE_ZERO, BigDecimal.ROUND_UP);
        paginationMap.put("totalPage", totalPage);//总页数
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("pagination", paginationMap);

        resultMap.put("list", keyToLowerCase(list));//结果集
        return resultMap;
    }

    /**
     * todo 后续再修改摒除这个方法
     * 将key的大写改为小写
     * @param list
     * @return
     */
    private List<Map<String, Object>> keyToLowerCase(List<Map<String, Object>> list) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        for(Map<String, Object> map : list){
            Map<String, Object> newMap = new HashMap<String, Object>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                if (map.get(key) instanceof Clob){//处理Clob字段
                    Clob clob = (Clob) map.get(key);
                    try {
                        newMap.put(key.toLowerCase(), clob.getSubString((long)1, (int) clob.length()));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }else{
                    newMap.put(key.toLowerCase(), map.get(key));
                }
            }
            result.add(newMap);
        }
        list.clear();//清空原有数据
        return result;
    }

    /**
     * 获取总记录（SQL）
     *
     * @param sql：SQL语句
     * @param paramMap：Map参数
     * @return：统计结果
     */
    private BigDecimal countBySQL(String sql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createSQLQuery("SELECT COUNT(1) FROM (" + sql + ") temp");
        return new BigDecimal(getFinalQuery(query, paramMap).uniqueResult().toString());
    }

    /**
     * 获取总记录（SQL）
     *
     * @param sql：SQL语句
     * @param paramMap：Map参数
     * @return：统计结果
     */
    public BigDecimal findCountBySQL(String sql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createSQLQuery(sql);
        return new BigDecimal(getFinalQuery(query, paramMap).uniqueResult().toString());
    }

    /**
     * 获取（SQL）
     *
     * @param sql：SQL语句
     * @param paramMap：Map参数
     * @return：
     */
    public int findBySQLInt(String sql, Map<String, Object> paramMap) {
        Query query = getCurrentSession().createSQLQuery(sql);
        return Integer.parseInt(getFinalQuery(query, paramMap).uniqueResult().toString());
    }

    /**
     * 更新操作的执行方法(HQL)
     *
     * @param hql：HQL语句
     * @param params：变参数组
     * @return：更新记录数
     */
    private int executeHQLUpdate(final String hql, final Object... params) {
        Query query = getCurrentSession().createQuery(hql);
        return getFinalQuery(query, params).executeUpdate();
    }

    /**
     * 更新操作的执行方法(HQL)
     *
     * @param hql：HQL语句
     * @param paramMap：Map集合参数
     * @return：更新记录数
     */
    private int executeHQLUpdate(final String hql, final Map<String, Object> paramMap) {
        Query query = getCurrentSession().createQuery(hql);
        return getFinalQuery(query, paramMap).executeUpdate();
    }

    /**
     * 更新操作的执行方法(SQL)
     *
     * @param sql：SQL语句
     * @param params：变参数组
     * @return：更新记录数
     */
    private int executeSQLUpdate(final String sql, final Object... params) {
        Query query = getCurrentSession().createSQLQuery(sql);
        return getFinalQuery(query, params).executeUpdate();
    }

    /**
     * 更新操作的执行方法(SQL)
     *
     * @param sql：SQL语句
     * @param paramMap：Map数组
     * @return：更新记录数
     */
    private int executeSQLUpdate(final String sql, final Map<String, Object> paramMap) {
        Query query = getCurrentSession().createSQLQuery(sql);
        return getFinalQuery(query, paramMap).executeUpdate();
    }

    /**
     * 赋值并获取最终的 Query 对象
     * 方法链编程风格
     *
     * @param query：Hibernate  Query 对象
     * @param paramMap：Map集合参数
     * @return ：赋值后的 Query 对象
     */
    private Query getFinalQuery(Query query, Map<String, Object> paramMap) {
        if(paramMap != null){
            for (Map.Entry<String, Object> entry : paramMap.entrySet()) {
                String key = entry.getKey();
                Object obj = entry.getValue();
                if (obj instanceof Collection<?>) {//集合
                    query.setParameterList(key, (Collection<?>) obj);
                } else if (obj instanceof Object[]) {//数组
                    query.setParameterList(key, (Object[]) obj);
                } else {//普通参数
                    query.setParameter(key, obj);
                }
            }
        }
        return query;
    }

    /**
     * 赋值并获取最终的 Query 对象
     *
     * @param query：Hibernate Query 对象
     * @param params：数组参数
     * @return：赋值后的 Query 对象
     */
    private Query getFinalQuery(Query query, Object... params) {
        for (int i = 0, n = params.length; i < n; i++) {
            query.setParameter(i, params[i]);
        }
        return query;
    }

}
