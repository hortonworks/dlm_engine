<#--
  #%L
  License Maven Plugin
  %%
  Copyright (C) 2012 Codehaus, Tony Chemit
  %%
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Lesser Public License for more details.

  You should have received a copy of the GNU General Lesser Public
  License along with this program.  If not, see
  <http://www.gnu.org/licenses/lgpl-3.0.html>.
  #L%
  -->
<#-- To render the third-party file.
 Available context :

 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)

 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->
<#function licenseFormat licenses>
    <#assign result = ""/>
    <#list licenses as license>
        <#assign result = result + " " + license />
    </#list>
    <#return result>
</#function>
<#function artifactFormat p>
    <#if p.name?index_of('Unnamed') &gt; -1>
        <#return p.artifactId + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
    <#else>
        <#return p.name + " (" + p.groupId + ":" + p.artifactId + ":" + p.version + " - " + (p.url!"no url defined") + ")">
    </#if>
</#function>

Hortonworks DataPlane Services, version 1.0
Portions Copyright 2016-2017 Hortonworks, Inc.  All rights reserved.

This product includes code developed and licensed by third parties (“Separately Licensed Code”).  Each of the components listed below is licensed under the applicable third-party agreements, which may be open source license agreements, and is licensed separately from Hortonworks licensed software and/or services. Where the Separately Licensed Code contains dual or multiple licensing options, the most permissive license is elected.

Notwithstanding any of the terms in the third party license agreement, Customer’s Terms of Use, or any other agreement Customer may have with Hortonworks: (A) HORTONWORKS PROVIDES SEPARATELY LICENSED CODE TO CUSTOMER WITHOUT WARRANTIES OF ANY KIND; (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO SEPARATELY LICENSED CODE, INCLUDING BUT NOT LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE; (C) HORTONWORKS IS NOT LIABLE TO CUSTOMER, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD CUSTOMER HARMLESS FOR ANY CLAIMS ARISING FROM OR RELATED TO SEPARATELY LICENSED CODE; AND (D) WITH RESPECT TO THE SEPARATELY LICENSED CODE, HORTONWORKS IS NOT LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY, LOSS OR CORRUPTION OF DATA.

To the extent required by the licenses associated with the Separately Licensed Code under which you receive only binaries, Hortonworks will provide a copy of the Separately Licensed Code’s source code upon request and at a reasonable fee.  All requests for source code should be made in a letter clearly identifying the components for which the source code is requested,  typically sent within three (3) years from the date you received the covered binary, and mailed to “Hortonworks Inc., ATTN: Legal Dept., 5470 Great America Pkwy, Santa Clara, CA 95054.” Your written request should include: (i) the name and version number of the covered binary, (ii) the version number of the Hortonworks product containing the covered binary, (iii) your name, (iv) your company or organization name (if applicable), (v) the license under which the source code must be provided, and (vi) your return mailing and email address (if available).

Separately Licensed Code may include portions or all of the following:

<#if dependencyMap?size == 0>
The project has no dependencies.
<#else>
    <#list dependencyMap as e>
        <#assign project = e.getKey()/>
        <#assign licenses = e.getValue()/>
    ${artifactFormat(project)} - ${licenseFormat(licenses)}
    </#list>
</#if>