<%@ include file="/WEB-INF/jsp/include.jsp"%>

<div><spring:message code="personSearchResults.resultsTitle" /></div>

<portlet:actionURL var="selectPersonUrl">
    <portlet:param name="execution" value="${flowExecutionKey}" />
</portlet:actionURL>
<form action="${selectPersonUrl}" method="post">
    <ul>
        <c:forEach var="displayResultsEntry" items="${queryDisplayResults}">
            <li>
                <input id="uid.${fn:escapeXml(displayResultsEntry.key)}" type="radio" name="uid" value="${fn:escapeXml(displayResultsEntry.key)}" />
                <label for="uid.${fn:escapeXml(displayResultsEntry.key)}"> ${fn:escapeXml(displayResultsEntry.value)}</label>
            </li>
        </c:forEach>
    </ul>
    
    <spring:message var="viewDetailsButtonText" code="personSearchResults.viewDetailsButton" />
    <input type="submit" class="button" name="_eventId_select" value="${viewDetailsButtonText}" />
    
    <spring:message var="selectAndGoButtonText" code="${personSearchResults_selectAndGoButtonTextKey}" />
    <input type="submit" class="button" name="_eventId_selectAndGo" value="${selectAndGoButtonText}" />
    
    <spring:message var="newSearchButtonText" code="personSearchResults.newSearchButton" />
    <input type="submit" class="button" name="_eventId_newSearch" value="${newSearchButtonText}" />
</form>