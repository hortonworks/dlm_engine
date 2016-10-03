package com.hortonworks.beacon.main;

import com.hortonworks.beacon.entity.ACL;
import com.hortonworks.beacon.entity.Cluster;
import com.hortonworks.beacon.entity.ClusterProperties;
import com.hortonworks.beacon.entity.EntityHelper;
import com.hortonworks.beacon.entity.TestClass;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by sramesh on 9/29/16.
 */
@Path("beacon/api")
public class EntityResource extends AbstractEntityManager {

    @POST
    @Path("submit/{cluster-name}")
//    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
//    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Consumes({MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_PLAIN})
    public String submit(
            @PathParam("cluster-name") String extensionName,
            @Context HttpServletRequest request) {

        try {
            Properties requestProperties = new Properties();
            requestProperties.load(request.getInputStream());

            Cluster c = new Cluster();
            c.setName(requestProperties.getProperty(ClusterProperties.NAME.getName()));
            c.setDescription(requestProperties.getProperty(ClusterProperties.DESCRIPTION.getName()));
            c.setColo(requestProperties.getProperty(ClusterProperties.COLO.getName()));
            c.setNameNodeUri(requestProperties.getProperty(ClusterProperties.NAMENODE_URI.getName()));
            c.setExecuteUri(requestProperties.getProperty(ClusterProperties.EXECUTE_URI.getName()));
            c.setWfEngineUri(requestProperties.getProperty(ClusterProperties.WF_ENGINE_URI.getName()));
            c.setMessagingUri(requestProperties.getProperty(ClusterProperties.MESSAGING_URI.getName()));
            c.setHs2Uri(requestProperties.getProperty(ClusterProperties.HS2_URI.getName()));
            c.setTags(requestProperties.getProperty(ClusterProperties.TAGS.getName()));
            c.setCustomProperties(EntityHelper.getCustomProperties(requestProperties, ClusterProperties.getClusterElements()));

            ACL acl = new ACL();
            acl.setOwner(requestProperties.getProperty(ClusterProperties.ACL_OWNER.getName()));
            acl.setGroup(requestProperties.getProperty(ClusterProperties.ACL_GROUP.getName()));
            acl.setPermission(requestProperties.getProperty(ClusterProperties.ACL_PERMISSION.getName()));
            c.setACL(acl);

            TestClass t = new TestClass();
            t.setTestId(100);
            t.setTestName(ClusterProperties.NAME.getName());
            c.setTest(t);

            try {
                super.submitInternal(c);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
        return "Extension job submitted successfully";
    }
}


//            YamlWriter writer = new YamlWriter(new FileWriter("cluster-primary.yml"));
//            writer.write(c);
//            writer.close();

//            YamlReader reader = new YamlReader(new FileReader("cluster-primary.yml"));
//            Cluster contact = reader.read(Cluster.class);
//            System.out.println(contact.getName());


//        } catch (IOException e) {
//            throw new RuntimeException(e.getMessage());
//        } catch (Exception e) {
////            LOG.error("Error when submitting extension job: ", e);
////            throw BeaconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
//            throw e;
//        }
////        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted successfully");
//        return "Extension job submitted successfully";
//    }
//}
