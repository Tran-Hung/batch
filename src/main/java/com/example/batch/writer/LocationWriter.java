package com.example.batch.writer;

import com.example.batch.beans.LocationBean;
import com.example.batch.entity.Location;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.example.batch.repository.LocationRepository;

import java.util.List;

@Component
@Slf4j
public class LocationWriter implements ItemWriter<LocationBean> {

    @Autowired
    private LocationRepository locationRepository;

    @Override
    public void write(List<? extends LocationBean> beans) {
        try {
            Location location;
            for (LocationBean bean : beans) {
                location = locationRepository.findByLocationId(bean.getLocationId());
                if (location == null) {
                    location = new Location();
                    location.setLocationId(bean.getLocationId());
                }
                location.setName(bean.getName());
                locationRepository.save(location);
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
