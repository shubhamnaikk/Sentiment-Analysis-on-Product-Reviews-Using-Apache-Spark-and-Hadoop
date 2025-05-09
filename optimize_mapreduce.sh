#!/bin/bash

# Script to optimize Hadoop job performance for Amazon reviews analysis

# Set Hadoop and YARN configurations for optimal performance
echo "Setting Hadoop and YARN configurations..."

# Memory settings
hadoop_conf_dir=${HADOOP_CONF_DIR:-/etc/hadoop/conf}

# Check if we have permissions to modify config
if [ -w "$hadoop_conf_dir" ]; then
    # Set mapreduce settings in mapred-site.xml
    cat > $hadoop_conf_dir/mapred-site.xml << EOF
<?xml version="1.0"?>
<configuration>
    <!-- General MapReduce settings -->
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>4096</value>
    </property>
    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx1638m</value>
    </property>
    <property>
        <name>mapreduce.reduce.java.opts</name>
        <value>-Xmx3276m</value>
    </property>
    
    <!-- Combiner settings to reduce network traffic -->
    <property>
        <name>mapreduce.map.combine.minspills</name>
        <value>3</value>
    </property>
    
    <!-- Compression settings -->
    <property>
        <name>mapreduce.map.output.compress</name>
        <value>true</value>
    </property>
    <property>
        <name>mapreduce.map.output.compress.codec</name>
        <value>org.apache.hadoop.io.compress.SnappyCodec</value>
    </property>
    
    <!-- Performance tuning -->
    <property>
        <name>mapreduce.task.io.sort.mb</name>
        <value>512</value>
    </property>
    <property>
        <name>mapreduce.task.io.sort.factor</name>
        <value>100</value>
    </property>
</configuration>
EOF

    # Set YARN settings in yarn-site.xml
    cat > $hadoop_conf_dir/yarn-site.xml << EOF
<?xml version="1.0"?>
<configuration>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>24576</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>8192</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>1024</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-pmem-ratio</name>
        <value>2.1</value>
    </property>
</configuration>
EOF

    echo "Configuration files updated successfully."
    
    # Restart the services if possible
    if command -v systemctl &> /dev/null; then
        echo "Restarting Hadoop services..."
        systemctl restart hadoop-mapreduce-historyserver
        systemctl restart hadoop-yarn-resourcemanager
        systemctl restart hadoop-yarn-nodemanager
    else
        echo "Please restart Hadoop services manually to apply changes."
    fi
    
else
    echo "No permission to modify Hadoop configuration. Please apply these settings manually:"
    echo "- Set mapreduce.map.memory.mb to 2048"
    echo "- Set mapreduce.reduce.memory.mb to 4096"
    echo "- Enable map output compression"
    echo "- Set task.io.sort.mb to 512"
    echo "- Set task.io.sort.factor to 100"
fi

# Create a Combiner for reducing network traffic
echo "#!/usr/bin/env python3
import sys

def main():
    current_key = None
    total = 0
    count = 0

    for line in sys.stdin:
        try:
            key, rating = line.strip().split('\t')
            rating = float(rating)

            if key == current_key:
                total += rating
                count += 1
            else:
                if current_key is not None:
                    print(f\"{current_key}\\t{total / count}\")
                current_key = key
                total = rating
                count = 1
        except ValueError:
            continue

    if current_key is not None:
        print(f\"{current_key}\\t{total / count}\")

if __name__ == \"__main__\":
    main()
" > combiner.py

chmod +x combiner.py

echo "Created combiner.py to reduce data between map and reduce phases"
echo "Use this combiner with the -combiner option in your Hadoop streaming command"

echo "Performance optimization settings applied"