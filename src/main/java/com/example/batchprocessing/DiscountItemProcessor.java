package com.example.batchprocessing;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

public class DiscountItemProcessor implements ItemProcessor<Discount, Discount> {

	private static final Logger log = LoggerFactory.getLogger(DiscountItemProcessor.class);

	@Override
	public Discount process(final Discount discount) {
		final String name = discount.name().toUpperCase();
		final Double percentage = Double.valueOf(discount.percentage());

		final Discount transformedDiscount = new Discount(name, percentage);

		log.info("Converting ({}) into ({})", discount, transformedDiscount);

		return transformedDiscount;
	}

}
