/**
 * Licensed to Jasig under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * Jasig licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.jasig.portal.events.aggr.popular;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CollectionJoin;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.ParameterExpression;
import javax.persistence.criteria.Root;

import org.jasig.portal.events.aggr.AggregationInterval;
import org.jasig.portal.events.aggr.DateDimension;
import org.jasig.portal.events.aggr.TimeDimension;
import org.jasig.portal.events.aggr.dao.jpa.DateDimensionImpl;
import org.jasig.portal.events.aggr.dao.jpa.DateDimensionImpl_;
import org.jasig.portal.events.aggr.groups.AggregatedGroupMapping;
import org.jasig.portal.jpa.BaseJpaDao;
import org.joda.time.DateMidnight;
import org.joda.time.LocalDate;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

/**
 * @author Eric Dalquist
 * @version $Revision$
 */
@Repository
public class SimpleJpaPopularPortletAggregationDao extends BaseJpaDao implements PopularPortletAggregationPrivateDao {

    private CriteriaQuery<PopularPortletAggregationImpl> findPopularPortletAggregationByDateTimeIntervalQuery;
    private CriteriaQuery<PopularPortletAggregationImpl> findPopularPortletAggregationByDateTimeIntervalGroupQuery;
    private CriteriaQuery<PopularPortletAggregationImpl> findPopularPortletAggregationsByDateRangeQuery;
    private ParameterExpression<TimeDimension> timeDimensionParameter;
    private ParameterExpression<DateDimension> dateDimensionParameter;
    private ParameterExpression<AggregationInterval> intervalParameter;
    private ParameterExpression<AggregatedGroupMapping> aggregatedGroupParameter;
    private ParameterExpression<Set> aggregatedGroupsParameter;
    private ParameterExpression<LocalDate> startDate;
    private ParameterExpression<LocalDate> endDate;
    
    private EntityManager entityManager;

    @PersistenceContext(unitName = "uPortalAggrEventsPersistence")
    public final void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Override
    protected EntityManager getEntityManager() {
        return this.entityManager;
    }
    

    @Override
    public void afterPropertiesSet() throws Exception {
        this.timeDimensionParameter = this.createParameterExpression(TimeDimension.class, "timeDimension");
        this.dateDimensionParameter = this.createParameterExpression(DateDimension.class, "dateDimension");
        this.intervalParameter = this.createParameterExpression(AggregationInterval.class, "interval");
        this.startDate = this.createParameterExpression(LocalDate.class, "startDate");
        this.endDate = this.createParameterExpression(LocalDate.class, "endDate");
        
        this.findPopularPortletAggregationByDateTimeIntervalQuery = this.createCriteriaQuery(new Function<CriteriaBuilder, CriteriaQuery<PopularPortletAggregationImpl>>() {
            @Override
            public CriteriaQuery<PopularPortletAggregationImpl> apply(CriteriaBuilder cb) {
                final CriteriaQuery<PopularPortletAggregationImpl> criteriaQuery = cb.createQuery(PopularPortletAggregationImpl.class);
                final Root<PopularPortletAggregationImpl> root = criteriaQuery.from(PopularPortletAggregationImpl.class);
                criteriaQuery.select(root);
                root.fetch(PopularPortletAggregationImpl_.uniqueFNames, JoinType.LEFT);
                criteriaQuery.where(
                        cb.and(
                            cb.equal(root.get(PopularPortletAggregationImpl_.dateDimension), dateDimensionParameter),
                            cb.equal(root.get(PopularPortletAggregationImpl_.timeDimension), timeDimensionParameter),
                            cb.equal(root.get(PopularPortletAggregationImpl_.interval), intervalParameter)
                        )
                    );
                
                return criteriaQuery;
            }
        });

        this.findPopularPortletAggregationByDateTimeIntervalGroupQuery = this.createCriteriaQuery(new Function<CriteriaBuilder, CriteriaQuery<PopularPortletAggregationImpl>>() {
            @Override
            public CriteriaQuery<PopularPortletAggregationImpl> apply(CriteriaBuilder cb) {
                final CriteriaQuery<PopularPortletAggregationImpl> criteriaQuery = cb.createQuery(PopularPortletAggregationImpl.class);
                final Root<PopularPortletAggregationImpl> root = criteriaQuery.from(PopularPortletAggregationImpl.class);
                criteriaQuery.select(root);
                root.fetch(PopularPortletAggregationImpl_.uniqueFNames, JoinType.LEFT);
                criteriaQuery.where(
                        cb.and(
                            cb.equal(root.get(PopularPortletAggregationImpl_.dateDimension), dateDimensionParameter),
                            cb.equal(root.get(PopularPortletAggregationImpl_.timeDimension), timeDimensionParameter)
                        )
                    );
                
                return criteriaQuery;
            }
        });

        
        this.findPopularPortletAggregationsByDateRangeQuery = this.createCriteriaQuery(new Function<CriteriaBuilder, CriteriaQuery<PopularPortletAggregationImpl>>() {
            @Override
            public CriteriaQuery<PopularPortletAggregationImpl> apply(CriteriaBuilder cb) {
                final CriteriaQuery<PopularPortletAggregationImpl> criteriaQuery = cb.createQuery(PopularPortletAggregationImpl.class);
                
                final Root<DateDimensionImpl> root = criteriaQuery.from(DateDimensionImpl.class);
                final CollectionJoin<DateDimensionImpl, PopularPortletAggregationImpl> popularPortletAggrJoin = root.join(DateDimensionImpl_.popularPortletAggregations, JoinType.LEFT);
                
                criteriaQuery.select(popularPortletAggrJoin);
                criteriaQuery.where(
                        cb.and(
                                cb.between(root.get(DateDimensionImpl_.date), startDate, endDate)
                        )
                );
                criteriaQuery.orderBy(cb.desc(root.get(DateDimensionImpl_.date)));
                
                return criteriaQuery;
            }
        });
    }
    
    @Override
    public List<PopularPortletAggregation> getPopularPortletAggregations(DateMidnight start, DateMidnight end, AggregationInterval interval, AggregatedGroupMapping... aggregatedGroupMapping) {
        final TypedQuery<PopularPortletAggregationImpl> query = this.createQuery(findPopularPortletAggregationsByDateRangeQuery);
        query.setParameter(this.startDate, start.toLocalDate());
        query.setParameter(this.endDate, end.toLocalDate());
        query.setParameter(this.intervalParameter, interval);
        query.setParameter(this.aggregatedGroupsParameter, ImmutableSet.copyOf(aggregatedGroupMapping));
        System.out.println("LAN - really...2");
        return new ArrayList<PopularPortletAggregation>(query.getResultList());
    }
    
    @Override
    public List<PopularPortletAggregation> getPopularPortletAggregations(DateMidnight start, DateMidnight end) {
        final TypedQuery<PopularPortletAggregationImpl> query = this.createQuery(findPopularPortletAggregationsByDateRangeQuery);
        query.setParameter(this.startDate, start.toLocalDate());
        query.setParameter(this.endDate, end.toLocalDate());
        System.out.println("LAN - QUERY: " + this.startDate.toString() + " , " + start.toLocalDate().toString());
        List results = query.getResultList();
        System.out.println("LAN - hmm... " + results.size()); 
        return new ArrayList<PopularPortletAggregation>(results);
    }

    
    @Override
    public Set<PopularPortletAggregationImpl> getPopularPortletAggregationsForInterval(DateDimension dateDimension, TimeDimension timeDimension, AggregationInterval interval) {
        final TypedQuery<PopularPortletAggregationImpl> query = this.createCachedQuery(this.findPopularPortletAggregationByDateTimeIntervalQuery);
        query.setParameter(this.dateDimensionParameter, dateDimension);
        query.setParameter(this.timeDimensionParameter, timeDimension);
        //query.setParameter(this.intervalParameter, interval);
        System.out.println("LAN - really...3");
        
        final List<PopularPortletAggregationImpl> results = query.getResultList();
        System.out.println("LAN - QUERY: " + this.startDate.toString() + " , " + dateDimension.toString() + " AND our size is " + results.size());
        return new LinkedHashSet<PopularPortletAggregationImpl>(results);
    }

    @Override
    public PopularPortletAggregationImpl getPopularPortletAggregation(DateDimension dateDimension, TimeDimension timeDimension, AggregationInterval interval, AggregatedGroupMapping aggregatedGroup) {
        final TypedQuery<PopularPortletAggregationImpl> query = this.createCachedQuery(this.findPopularPortletAggregationByDateTimeIntervalGroupQuery);
        query.setParameter(this.dateDimensionParameter, dateDimension);
        query.setParameter(this.timeDimensionParameter, timeDimension);
        query.setParameter(this.intervalParameter, interval);
        query.setParameter(this.aggregatedGroupParameter, aggregatedGroup);
        System.out.println("LAN - really...4");
        final List<PopularPortletAggregationImpl> results = query.getResultList();
        return DataAccessUtils.uniqueResult(results);
    }
    
    @Transactional("aggrEvents")
    @Override
    public PopularPortletAggregationImpl createPopularPortletAggregation(DateDimension dateDimension, TimeDimension timeDimension, AggregationInterval interval, AggregatedGroupMapping aggregatedGroup) {
        final PopularPortletAggregationImpl popularPortletAggregation = new PopularPortletAggregationImpl(timeDimension, dateDimension, interval, aggregatedGroup);
        
        this.entityManager.persist(popularPortletAggregation);
        
        return popularPortletAggregation;
    }
    
    @Transactional("aggrEvents")
    @Override
    public void updatePopularPortletAggregation(PopularPortletAggregationImpl popularPortletAggregation) {
        this.entityManager.persist(popularPortletAggregation);
    }

    /*@Override
    public void updatePopularPortletReport(String fName)
    {
        if (this.uniqueFNames.add(fName)) {
            this.uniquePortletFNameCount++;
        }
        this.addCount++;
        int prevCount = this.addCountPerFName.get(fName);
        this.addCountPerFName.put(fName, new Integer(prevCount+1));
    }*/
}
