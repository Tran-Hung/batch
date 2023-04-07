package com.example.batch.repository;

import com.example.batch.entity.Location;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LocationRepository extends JpaRepository<Location, Long> {
    Location findByLocationId(String id);
}
