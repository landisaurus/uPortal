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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.jasig.portal.events.PortalEvent;
import org.jasig.portal.events.PortletAddedToLayoutPortalEvent;
import org.jasig.portal.events.aggr.DateDimension;
import org.jasig.portal.events.aggr.IPortalEventAggregator;
import org.jasig.portal.events.aggr.AggregationInterval;
import org.jasig.portal.events.aggr.AggregationIntervalInfo;
import org.jasig.portal.events.aggr.TimeDimension;
import org.jasig.portal.events.aggr.groups.AggregatedGroupMapping;
import org.jasig.portal.events.aggr.session.EventSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Event aggregator that uses {@link PopularPortletAggregationPrivateDao} to aggregate popularPortlet events 
 * 
 */
@Service
public class PopularPortletPortalEventAggregator implements IPortalEventAggregator<PortletAddedToLayoutPortalEvent> {
    private PopularPortletAggregationPrivateDao popularPortletAggregationDao;

    @Override
    public boolean supports(Class<? extends PortalEvent> type) {
        return PortletAddedToLayoutPortalEvent.class.isAssignableFrom(type);
    }

    @Autowired
    public void setPopularPortletAggregationDao(PopularPortletAggregationPrivateDao popularPortletAggregationDao) {
        this.popularPortletAggregationDao = popularPortletAggregationDao;
    }

    @Transactional("aggrEvents")
    @Override
    public void aggregateEvent(PortletAddedToLayoutPortalEvent e, EventSession eventSession, Map<AggregationInterval, AggregationIntervalInfo> currentIntervals) {
        
        for (Map.Entry<AggregationInterval, AggregationIntervalInfo> intervalInfoEntry : currentIntervals.entrySet()) {
            final AggregationInterval interval = intervalInfoEntry.getKey();
            final AggregationIntervalInfo intervalInfo = intervalInfoEntry.getValue();
            final DateDimension dateDimension = intervalInfo.getDateDimension();
            final TimeDimension timeDimension = intervalInfo.getTimeDimension();
            
            final Set<AggregatedGroupMapping> groupMappings = new LinkedHashSet<AggregatedGroupMapping>(eventSession.getGroupMappings());
            final Set<PopularPortletAggregationImpl> popularPortletAggregations = this.popularPortletAggregationDao.getPopularPortletAggregationsForInterval(dateDimension, timeDimension, interval);
            for (final PopularPortletAggregationImpl popularPortletAggregation : popularPortletAggregations) {
                //Remove the aggregation from the group set to mark that it has been updated
                groupMappings.remove(popularPortletAggregation.getAggregatedGroup());
                updateAggregation(e, intervalInfo, popularPortletAggregation);
            }
            //Create any left over groups
            if (!groupMappings.isEmpty()) {
                for (final AggregatedGroupMapping aggregatedGroup : groupMappings) {
                    final PopularPortletAggregationImpl popularPortletAggregation = popularPortletAggregationDao.createPopularPortletAggregation(dateDimension, timeDimension, interval, aggregatedGroup);
                    updateAggregation(e, intervalInfo, popularPortletAggregation);
                }
            }
        }
    }

    private void updateAggregation(PortletAddedToLayoutPortalEvent e, final AggregationIntervalInfo intervalInfo, final PopularPortletAggregationImpl popularPortletAggregation) {
        final String userName = e.getUserName();
        final String fName = e.getFname();
        final int duration = intervalInfo.getDurationTo(e.getTimestampAsDate());
        popularPortletAggregation.setDuration(duration);
        popularPortletAggregation.countPortletAdd(fName);
    }

    @Transactional("aggrEvents")
    @Override
    public void handleIntervalBoundary(AggregationInterval interval, Map<AggregationInterval, AggregationIntervalInfo> intervals) {
        final AggregationIntervalInfo intervalInfo = intervals.get(interval);
        final DateDimension dateDimension = intervalInfo.getDateDimension();
        final TimeDimension timeDimension = intervalInfo.getTimeDimension();
        
        final Set<PopularPortletAggregationImpl> popularPortletAggregations = this.popularPortletAggregationDao.getPopularPortletAggregationsForInterval(dateDimension, timeDimension, interval);
        for (final PopularPortletAggregationImpl popularPortletAggregation : popularPortletAggregations) {
            final int duration = intervalInfo.getTotalDuration();
            popularPortletAggregation.intervalComplete(duration);
            this.popularPortletAggregationDao.updatePopularPortletAggregation(popularPortletAggregation);
        }
    }
}
