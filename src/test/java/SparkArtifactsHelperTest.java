import com.gridu.spark.helpers.SparkArtifactsHelper;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkArtifactsHelperTest {

    @Test
    public void shouldCreateASparkContextWithAppNameTest(){
        assertThat(SparkArtifactsHelper.createSparkContext("local[*]","test").appName())
                .isEqualTo("test");
    }

    @Test
    public void shouldCreateASparkSessionWithAppNametest(){
        assertThat(SparkArtifactsHelper.createSparkSession("local[*]","test")
                .sparkContext().appName()).isEqualTo("test");
    }

    @Test
    public void shouldCreateAJavaStreamingContextWithDuration3000Ms(){
        assertThat(SparkArtifactsHelper.createJavaStreamingContext("local[*]","test",3)
                    .ssc().checkpointDuration()).isEqualTo(3000);
    }

}
