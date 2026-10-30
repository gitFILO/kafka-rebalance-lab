package lab;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

// lab 도메인 MBean. JMX exporter(monitoring/jmx/client.yml)가 lab_<type>_<attribute>로 노출한다.
final class LabMetrics implements DynamicMBean {

    private static final Map<String, LabMetrics> BEANS = new ConcurrentHashMap<>();

    private final Map<String, AtomicLong> values = new ConcurrentHashMap<>();

    private LabMetrics() {
    }

    static LabMetrics of(String keyProperties) {
        return BEANS.computeIfAbsent(keyProperties, k -> {
            LabMetrics bean = new LabMetrics();
            try {
                ManagementFactory.getPlatformMBeanServer().registerMBean(bean, new ObjectName("lab:" + k));
            } catch (Exception e) {
                throw new IllegalStateException("MBean register failed: " + k, e);
            }
            return bean;
        });
    }

    void set(String attribute, long value) {
        values.computeIfAbsent(attribute, a -> new AtomicLong()).set(value);
    }

    void add(String attribute, long delta) {
        values.computeIfAbsent(attribute, a -> new AtomicLong()).addAndGet(delta);
    }

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException {
        AtomicLong v = values.get(attribute);
        if (v == null) {
            throw new AttributeNotFoundException(attribute);
        }
        return v.get();
    }

    @Override
    public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();
        for (String a : attributes) {
            AtomicLong v = values.get(a);
            if (v != null) {
                list.add(new Attribute(a, v.get()));
            }
        }
        return list;
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        return new AttributeList();
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        MBeanAttributeInfo[] attrs = values.keySet().stream()
            .sorted()
            .map(a -> new MBeanAttributeInfo(a, "long", a, true, false, false))
            .toArray(MBeanAttributeInfo[]::new);
        return new MBeanInfo(LabMetrics.class.getName(), "lab metrics", attrs, null, null, null);
    }
}
