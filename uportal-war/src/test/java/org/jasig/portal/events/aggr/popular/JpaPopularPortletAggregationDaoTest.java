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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import javax.naming.CompositeName;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.jasig.portal.concurrency.CallableWithoutResult;
import org.jasig.portal.events.aggr.DateDimension;
import org.jasig.portal.events.aggr.AggregationInterval;
import org.jasig.portal.events.aggr.TimeDimension;
import org.jasig.portal.events.aggr.dao.DateDimensionDao;
import org.jasig.portal.events.aggr.dao.TimeDimensionDao;
import org.jasig.portal.events.aggr.groups.AggregatedGroupLookupDao;
import org.jasig.portal.events.aggr.groups.AggregatedGroupMapping;
import org.jasig.portal.groups.ICompositeGroupService;
import org.jasig.portal.groups.IEntityGroup;
import org.jasig.portal.test.BaseJpaDaoTest;
import org.joda.time.DateMidnight;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Eric Dalquist
 * @version $Revision$
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:jpaAggrEventsTestContext.xml")
public class JpaPopularPortletAggregationDaoTest extends BaseJpaDaoTest {
    @Autowired
    private AggregatedGroupLookupDao aggregatedGroupLookupDao;
    @Autowired
    private PopularPortletAggregationPrivateDao popularPortletAggregationDao;
    @Autowired
    private TimeDimensionDao timeDimensionDao;
    @Autowired
    private DateDimensionDao dateDimensionDao;
    @Autowired
    private ICompositeGroupService compositeGroupService;
    
    @PersistenceContext(unitName = "uPortalAggrEventsPersistence")
    private EntityManager entityManager;
    
    @Override
    protected EntityManager getEntityManager() {
        return this.entityManager;
    }
    
    @Test
    public void testPopularPortletAggregationLifecycle() throws Exception {
        final IEntityGroup entityGroupA = mock(IEntityGroup.class);
        when(entityGroupA.getServiceName()).thenReturn(new CompositeName("local"));
        when(entityGroupA.getName()).thenReturn("Group A");
        when(compositeGroupService.findGroup("local.0")).thenReturn(entityGroupA);
        
        final IEntityGroup entityGroupB = mock(IEntityGroup.class);
        when(entityGroupB.getServiceName()).thenReturn(new CompositeName("local"));
        when(entityGroupB.getName()).thenReturn("Group B");
        when(compositeGroupService.findGroup("local.1")).thenReturn(entityGroupB);
        
        
        final DateTime instant = new DateTime(1326734644000l); //just a random time
        final DateMidnight instantDate = instant.toDateMidnight();
        final LocalTime instantTime = instant.toLocalTime();
        
        this.executeInTransaction(new CallableWithoutResult() {
            @Override
            protected void callWithoutResult() {
                dateDimensionDao.createDateDimension(instantDate, 0, null);
                timeDimensionDao.createTimeDimension(instantTime);
            }
        });
        
        this.executeInTransaction(new CallableWithoutResult() {
            @Override
            protected void callWithoutResult() {
                final DateDimension dateDimension = dateDimensionDao.getDateDimensionByDate(instantDate);
                final TimeDimension timeDimension = timeDimensionDao.getTimeDimensionByTime(instantTime);
                final AggregatedGroupMapping groupA = aggregatedGroupLookupDao.getGroupMapping("local.0");
                final AggregatedGroupMapping groupB = aggregatedGroupLookupDao.getGroupMapping("local.1");
                
                final PopularPortletAggregationImpl popularPortletAggregationFiveMinuteGroupA = popularPortletAggregationDao.createPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE, groupA);
                final PopularPortletAggregationImpl popularPortletAggregationFiveMinuteGroupB = popularPortletAggregationDao.createPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE, groupB);
                final PopularPortletAggregationImpl popularPortletAggregationHour = popularPortletAggregationDao.createPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.HOUR, groupA);

                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("joe");
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("john");
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("levi");
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("erin");
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("john");
                popularPortletAggregationFiveMinuteGroupA.setDuration(1);
                
                popularPortletAggregationFiveMinuteGroupB.countPortletAdd("joe");
                popularPortletAggregationFiveMinuteGroupB.countPortletAdd("john");
                popularPortletAggregationFiveMinuteGroupB.setDuration(1);
                
                popularPortletAggregationHour.countPortletAdd("joe");
                popularPortletAggregationHour.countPortletAdd("john");
                popularPortletAggregationHour.countPortletAdd("levi");
                popularPortletAggregationHour.countPortletAdd("erin");
                popularPortletAggregationHour.countPortletAdd("john");
                popularPortletAggregationHour.setDuration(1);
                
                popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregationFiveMinuteGroupA);
                popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregationHour);
            }
        });
        

        
        this.execute(new CallableWithoutResult() {
            @Override
            protected void callWithoutResult() {
                final DateDimension dateDimension = dateDimensionDao.getDateDimensionByDate(instantDate);
                final TimeDimension timeDimension = timeDimensionDao.getTimeDimensionByTime(instantTime);
                final AggregatedGroupMapping groupA = aggregatedGroupLookupDao.getGroupMapping("local.0");
                
                final Set<PopularPortletAggregationImpl> popularPortletAggregationsFiveMinute = popularPortletAggregationDao.getPopularPortletAggregationsForInterval(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE);
                assertEquals(2, popularPortletAggregationsFiveMinute.size());
                
                for (final PopularPortletAggregationImpl popularPortletAggregation : popularPortletAggregationsFiveMinute) {
                    if (popularPortletAggregation.getAggregatedGroup().equals(groupA)) {
                        assertEquals(5, popularPortletAggregation.getAddCount());
                        assertEquals(4, popularPortletAggregation.getUniqueAddCount());
                    }
                    else {
                        assertEquals(2, popularPortletAggregation.getAddCount());
                        assertEquals(2, popularPortletAggregation.getUniqueAddCount());
                    }
                }
                
                
                final Set<PopularPortletAggregationImpl> popularPortletAggregationsHour = popularPortletAggregationDao.getPopularPortletAggregationsForInterval(dateDimension, timeDimension, AggregationInterval.HOUR);
                assertEquals(1, popularPortletAggregationsHour.size());
                
                final PopularPortletAggregationImpl popularPortletAggregation = popularPortletAggregationsHour.iterator().next();
                assertEquals(5, popularPortletAggregation.getAddCount());
                assertEquals(4, popularPortletAggregation.getUniqueAddCount());
            }
        });
        

        
        this.executeInTransaction(new CallableWithoutResult() {
            @Override
            protected void callWithoutResult() {
                final DateDimension dateDimension = dateDimensionDao.getDateDimensionByDate(instantDate);
                final TimeDimension timeDimension = timeDimensionDao.getTimeDimensionByTime(instantTime);
                final AggregatedGroupMapping groupA = aggregatedGroupLookupDao.getGroupMapping("local.0");
                final AggregatedGroupMapping groupB = aggregatedGroupLookupDao.getGroupMapping("local.1");

                final PopularPortletAggregationImpl popularPortletAggregationFiveMinuteGroupA = popularPortletAggregationDao.getPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE, groupA);
                final PopularPortletAggregationImpl popularPortletAggregationFiveMinuteGroupB = popularPortletAggregationDao.getPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE, groupB);
                final PopularPortletAggregationImpl popularPortletAggregationHour = popularPortletAggregationDao.getPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.HOUR, groupA);
                
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("john");
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("elvira");
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("levi");
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("gretchen");
                popularPortletAggregationFiveMinuteGroupA.countPortletAdd("erin");
                popularPortletAggregationFiveMinuteGroupA.setDuration(2);
                
                popularPortletAggregationFiveMinuteGroupB.countPortletAdd("gretchen");
                popularPortletAggregationFiveMinuteGroupB.setDuration(2);
                
                popularPortletAggregationHour.countPortletAdd("john");
                popularPortletAggregationHour.countPortletAdd("elvira");
                popularPortletAggregationHour.countPortletAdd("levi");
                popularPortletAggregationHour.countPortletAdd("gretchen");
                popularPortletAggregationHour.countPortletAdd("erin");
                popularPortletAggregationHour.setDuration(2);
                
                popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregationFiveMinuteGroupA);
                popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregationFiveMinuteGroupB);
                popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregationHour);
            }
        });

        this.execute(new CallableWithoutResult() {
            @Override
            protected void callWithoutResult() {
                final DateDimension dateDimension = dateDimensionDao.getDateDimensionByDate(instantDate);
                final TimeDimension timeDimension = timeDimensionDao.getTimeDimensionByTime(instantTime);
                final AggregatedGroupMapping groupA = aggregatedGroupLookupDao.getGroupMapping("local.0");

                
                final Set<PopularPortletAggregationImpl> popularPortletAggregationsFiveMinute = popularPortletAggregationDao.getPopularPortletAggregationsForInterval(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE);
                assertEquals(2, popularPortletAggregationsFiveMinute.size());
                
                for (final PopularPortletAggregationImpl popularPortletAggregation : popularPortletAggregationsFiveMinute) {
                    if (popularPortletAggregation.getAggregatedGroup().equals(groupA)) {
                        assertEquals(10, popularPortletAggregation.getAddCount());
                        assertEquals(6, popularPortletAggregation.getUniqueAddCount());
                    }
                    else {
                        assertEquals(3, popularPortletAggregation.getAddCount());
                        assertEquals(3, popularPortletAggregation.getUniqueAddCount());
                    }
                }
                
                
                final Set<PopularPortletAggregationImpl> popularPortletAggregationsHour = popularPortletAggregationDao.getPopularPortletAggregationsForInterval(dateDimension, timeDimension, AggregationInterval.HOUR);
                assertEquals(1, popularPortletAggregationsHour.size());
                
                final PopularPortletAggregationImpl popularPortletAggregation = popularPortletAggregationsHour.iterator().next();
                assertEquals(10, popularPortletAggregation.getAddCount());
                assertEquals(6, popularPortletAggregation.getUniqueAddCount());
            }
        });

        this.executeInTransaction(new CallableWithoutResult() {
            @Override
            protected void callWithoutResult() {
                final DateDimension dateDimension = dateDimensionDao.getDateDimensionByDate(instantDate);
                final TimeDimension timeDimension = timeDimensionDao.getTimeDimensionByTime(instantTime);
                final AggregatedGroupMapping groupA = aggregatedGroupLookupDao.getGroupMapping("local.0");
                final AggregatedGroupMapping groupB = aggregatedGroupLookupDao.getGroupMapping("local.1");

                final PopularPortletAggregationImpl popularPortletAggregationFiveMinuteGroupA = popularPortletAggregationDao.getPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE, groupA);
                final PopularPortletAggregationImpl popularPortletAggregationFiveMinuteGroupB = popularPortletAggregationDao.getPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE, groupB);
                final PopularPortletAggregationImpl popularPortletAggregationHour = popularPortletAggregationDao.getPopularPortletAggregation(dateDimension, timeDimension, AggregationInterval.HOUR, groupA);
                
                popularPortletAggregationFiveMinuteGroupA.intervalComplete(5);
                popularPortletAggregationFiveMinuteGroupB.intervalComplete(5);
                popularPortletAggregationHour.intervalComplete(60);
                
                popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregationFiveMinuteGroupA);
                popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregationFiveMinuteGroupB);
                popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregationHour);
            }
        });

        this.execute(new CallableWithoutResult() {
            @Override
            protected void callWithoutResult() {
                final DateDimension dateDimension = dateDimensionDao.getDateDimensionByDate(instantDate);
                final TimeDimension timeDimension = timeDimensionDao.getTimeDimensionByTime(instantTime);
                final AggregatedGroupMapping groupA = aggregatedGroupLookupDao.getGroupMapping("local.0");
                
                final Set<PopularPortletAggregationImpl> popularPortletAggregationsFiveMinute = popularPortletAggregationDao.getPopularPortletAggregationsForInterval(dateDimension, timeDimension, AggregationInterval.FIVE_MINUTE);
                assertEquals(2, popularPortletAggregationsFiveMinute.size());
                
                for (final PopularPortletAggregationImpl popularPortletAggregation : popularPortletAggregationsFiveMinute) {
                    if (popularPortletAggregation.getAggregatedGroup().equals(groupA)) {
                        assertEquals(10, popularPortletAggregation.getAddCount());
                        assertEquals(6, popularPortletAggregation.getUniqueAddCount());
                    }
                    else {
                        assertEquals(3, popularPortletAggregation.getAddCount());
                        assertEquals(3, popularPortletAggregation.getUniqueAddCount());
                    }
                }
                
                
                final Set<PopularPortletAggregationImpl> popularPortletAggregationsHour = popularPortletAggregationDao.getPopularPortletAggregationsForInterval(dateDimension, timeDimension, AggregationInterval.HOUR);
                assertEquals(1, popularPortletAggregationsHour.size());
                
                final PopularPortletAggregationImpl popularPortletAggregation = popularPortletAggregationsHour.iterator().next();
                assertEquals(10, popularPortletAggregation.getAddCount());
                assertEquals(6, popularPortletAggregation.getUniqueAddCount());
            }
        });

        this.execute(new CallableWithoutResult() {
            @Override
            protected void callWithoutResult() {
                final DateDimension dateDimension = dateDimensionDao.getDateDimensionByDate(instantDate);
                final TimeDimension timeDimension = timeDimensionDao.getTimeDimensionByTime(instantTime);
                final AggregatedGroupMapping groupA = aggregatedGroupLookupDao.getGroupMapping("local.0");
                
                final List<PopularPortletAggregation> popularPortletAggregations = popularPortletAggregationDao.getPopularPortletAggregations(
                        instantDate.monthOfYear().roundFloorCopy(),
                        instantDate.monthOfYear().roundCeilingCopy(),
                        AggregationInterval.FIVE_MINUTE,
                        groupA);
                        
                        
                assertEquals(1, popularPortletAggregations.size());
            }
        });
    }
}
