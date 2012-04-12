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

package org.jasig.portal.rest;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Resource;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jasig.portal.EntityIdentifier;
import org.jasig.portal.events.aggr.popular.PopularPortletAggregation;
import org.jasig.portal.events.aggr.popular.PopularPortletAggregationDao;
import org.jasig.portal.security.AdminEvaluator;
import org.jasig.portal.security.IAuthorizationPrincipal;
import org.jasig.portal.security.IPerson;
import org.jasig.portal.security.IPersonManager;
import org.jasig.portal.services.AuthorizationService;
import org.jasig.portal.utils.cache.CacheFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;
import org.joda.time.DateMidnight;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;

/**
 * Spring controller that returns a JSON representation of how many times users 
 * have either added each portlet in the specified number of days, counting 
 * backwards from the specified day (inclusive). 
 * <p>Request parameters:</p>
 * <ul>
 *   <li>days: Number of calendar days to include in the report; default is 30</li>
 *   <li>fromDate: Date (inclusive) from which to count backwards; default is today</li>
 * </ul>
 *
 * @author Drew Wills, drew@unicon.net
 */
@Controller
public class StatsLayoutModificationsController implements InitializingBean {
    
    private static final int MIN_DAYS = 0;
    
    //private EventCountFactory factory;
    private int maxDays = 365;  // default
    private final Pattern datePattern = Pattern.compile("\\d+/\\d+/\\d+");
    //private final DateFormat format = DateFormat.getDateInstance(DateFormat.SHORT);
    private final DateTimeFormatter format = DateTimeFormat.shortDate();
    private CacheFactory cacheFactory;
    private PopularPortletAggregationDao eventAggr;
    private final Log log = LogFactory.getLog(getClass());
    
    public void setMaxDays(int maxDays) {
        this.maxDays = maxDays;
    }
    
    @Resource(name="popularPortletAggregationDao")
    public void setPopularPortletsEventsAggregator(PopularPortletAggregationDao eventAggr)
    {
        this.eventAggr = eventAggr;
    }

    @Autowired
    public void setCacheFactory(CacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        //this.factory = new EventCountFactory(dataSource, cacheFactory);
    }

    @RequestMapping(value="/userLayoutModificationsCounts")
    public ModelAndView getEventCounts(HttpServletRequest req, HttpServletResponse res) throws ServletException {
        
System.out.println("it has begun");
        // Days parameter
        int days = 30;  // default
        if (req.getParameter("days") != null) {
            String daysParam = req.getParameter("days").trim();
            try {
                days = Integer.parseInt(daysParam);
            } catch (NumberFormatException nfe) {
                String msg = "Unrecognizable days parameter (must be a valid integer): " + daysParam;
                log.warn(msg, nfe);
                throw new ServletException(msg, nfe);
            }
        }
        
        DateMidnight end = DateMidnight.now();
        DateMidnight start = end.minus(Duration.standardDays(days));

        if (req.getParameter("fromDate") != null) {
            String fromDateParam = req.getParameter("fromDate").trim();
            // If the user doesn't enter a date, the UI sends "today" (or other 
            // string), so ignore anything that's not even close...
            if (datePattern.matcher(fromDateParam).matches()) {
                try {
                    DateMidnight beforeThisTime = new DateMidnight(format.parseDateTime(fromDateParam));
                    end = beforeThisTime;  // if it failed to formatter, it should already be a caught exception. 
                } catch (IllegalArgumentException pe) {
                    // Passing a bad date is ok, it just results in the default
                    if (log.isInfoEnabled()) {
                        String msg = "Unrecognizable fromDate parameter (format 'mm/dd/yyyy'): " + fromDateParam;
                        log.info(msg, pe);
                    }
                }
            }
        }

        // Be certain days is within prescribed limits
        if (days < MIN_DAYS) {
            days = MIN_DAYS;
        } else if (days > maxDays) {
            days = maxDays;
        }
        
        int sizeLimit = 100;// LAN - move this to prefreences
System.out.println("LAN - we have dates " + start.toString() + ", " + end.toString());
        List<PopularPortletAggregation> fetchedData = eventAggr.getPopularPortletAggregations(start, end);
        System.out.println("LAN - " + fetchedData.size() + " is size.  Prepare to convert.");
        List<CountingTuple> completeList = convertToUserFacing(fetchedData, sizeLimit);

        //IPerson user = personManager.getPerson(req);
        //List<CountingTuple> filteredList = filterByPermissions(user, completeList);
        
        return new ModelAndView("jsonView", "counts", completeList);

    }
    
    public List<CountingTuple> convertToUserFacing(List<PopularPortletAggregation> query, int maxSize)
    {
        System.out.println("LAN - " + query.size() + " Is in the covert chamber");
        List<CountingTuple> rslt=new ArrayList();
        List<String> masterFNameList = new ArrayList();
        int i = 0;
        for (PopularPortletAggregation currentAggr : query)
        {
            if (currentAggr != null) {
                System.out.println(i + " is important. " + currentAggr.toString());
                i++;
                List<String> currentFNames = currentAggr.getUniqueFNames();
                if (currentFNames.size() > 0)
                {
                    System.out.println ("LAN - size is greater than 0");
                    for (String cFName : currentFNames)
                    { 
                        if (masterFNameList.contains(cFName))
                        {
                            System.out.println("LAN - it has the name " + cFName);
                            for (CountingTuple currentTuple: rslt)
                            {
                                currentTuple.setCount(currentTuple.getCount()+currentAggr.getCountByFName(cFName));
                            }
                        } else
                        {
                            System.out.println("LAN - it does NOT have the name " + cFName);
                            rslt.add(new CountingTuple(i, cFName , "Title: cFName", "Description", currentAggr.getCountByFName(cFName)));
                            masterFNameList.add(cFName);
                        }
                        
                    }
                }
                else
                {
                    System.out.println("LAN - the list isn't big enough");
                }
            } // this should be removed
            else { System.out.println(" wait...   WTF, how is it null!!!"); }
        }
        if (query.size() > maxSize)
        {
            System.out.println("REDUCING SIZE NEEDED ");
        }
        return rslt;
    }

    /*
     * Implementation
     */
    
    private List<CountingTuple> filterByPermissions(IPerson user, List<CountingTuple> completeList) {
        
        // Assertions
        if (user == null) {
            String msg = "Argument 'user' cannot be null";
            throw new IllegalArgumentException(msg);
        }
        if (completeList == null) {
            String msg = "Argument 'completeList' cannot be null";
            throw new IllegalArgumentException(msg);
        }
        
        if(AdminEvaluator.isAdmin(user)) {
            // Admins may see the complete list
            return completeList;
        }
        
        EntityIdentifier ei = user.getEntityIdentifier();
        IAuthorizationPrincipal ap = AuthorizationService.instance().newPrincipal(ei.getKey(), ei.getType());

        List<CountingTuple> rslt = new ArrayList<CountingTuple>();
        for (CountingTuple tuple : completeList) {
            if (ap.canSubscribe(String.valueOf(tuple.getId()))) {
                rslt.add(tuple);
            }
        }
        
        return rslt;
        
    }
    
    public static final class CountingTuple implements Comparable<CountingTuple> {
        
        private final int id;
        private final String portletFName;
        private final String portletTitle;
        private String portletDescription = "[no description available]";  // default
        private int count;
        
        public CountingTuple(int id, String portletFName, String portletTitle, String portletDescription, int count) {

            // Assertions
            if (portletFName == null) {
                String msg = "Argument 'portletFName' cannot be null";
                throw new IllegalArgumentException(msg);
            }
            if (portletTitle == null) {
                String msg = "Argument 'portletTitle' cannot be null";
                throw new IllegalArgumentException(msg);
            }
            // NB:  'portletDescription' actually can be null

            this.id = id;
            this.portletFName = portletFName;
            this.portletTitle = portletTitle;
            if (portletDescription != null) {
                this.portletDescription = portletDescription;
            }
            this.count = count;
        }

        public int getId() {
            return id;
        }

        public String getPortletFName() {
            return portletFName;
        }

        public String getPortletTitle() {
            return portletTitle;
        }

        public String getPortletDescription() {
            return portletDescription;
        }

        public int getCount() {
            return count;
        }
        
        public void setCount(int c)
        {
            count = c;
        }

        @Override
        public int compareTo(CountingTuple tuple) {
            // Natural order for these is count
            return new Integer(count).compareTo(tuple.getCount());
        }
        
    }
}
