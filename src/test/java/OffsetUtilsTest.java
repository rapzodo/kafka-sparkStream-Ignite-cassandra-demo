import com.gridu.stopbot.spark.processing.utils.OffsetUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.junit.Test;
import static org.junit.Assert.*;

public class OffsetUtilsTest {

    @Test
    public void testCreateOffsets(){
        int partition = 0;
        String topic = "sometopic";
        long start = 0;
        long limit = 100;
        int length = 2;

        OffsetRange[] offsetRanges = OffsetUtils.createOffsetRanges(topic, partition, start, limit, length);

        assertEquals(2,offsetRanges.length);
        assertEquals(topic,offsetRanges[1].topic());
        assertEquals(partition,offsetRanges[1].partition());
        assertEquals(start,offsetRanges[1].fromOffset());
        assertEquals(limit,offsetRanges[1].untilOffset());

    }

}
