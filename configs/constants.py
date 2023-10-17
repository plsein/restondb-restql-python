
DEFAULT_JWT_ACCESS_TOKEN_EXPIRES_MINS = 720

FILTER_TYPES = ['eq', 'ne', 'like', 'ilike', 'is_null', 'in', 'not_in', 'lt', 'lte', 'gt', 'gte', 'range', 'contains', 'contained_by', 'overlap']

EXAMPLE_QUERY = """

query getData {

  zones(
    sort: [ZONE_ID_ASC, ZONE_NAME_ASC], 
    filters: {zone_name_ilike:"af%", and: [{zone_id_not_in:[0]}]},
    aggs: {
      aggregation:"sum", 
      field:"zone_id", 
      group:["zones.zone_id","zones.zone_name"], 
      having:"sum(zones.zone_id) > 0"
    }
    first:2, 
    after:"YXJyYXljb25uZWN0aW9uOjA=",
    page:1,
    per_page:3
  ) {

    total_count
    edge_count

    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
    }

    edges {
      cursor
      node {
        pk
        id
        zone_id
        zone_name
        cloud_regions {
          region_id
          region_name
          cloud_service_providers {
            provider_id
            provider_name
          }
        }
      }
    }

    custom(
      fields:["sum(zone_id) as sum_zone_id", "sum(zone_id)/2 as half_sum_zone_id", "cloud_regions.region_name as region_name", "cloud_service_providers.provider_name"],
      table:["zones"],
      innerJoin: ["cloud_regions"],
      leftJoin: ["cloud_service_providers"],
      where:["zone_id > 0"],
      group:["zone_id", "cloud_regions.region_name", "cloud_service_providers.provider_name"],
      having:["sum(zone_id) > 100"],
      sort: ["zone_name asc"],
      limit: ["2"],
      offset: ["1"]
    )

  }

}

mutation editData {
  updatezones1: updatezones(ids:[3], input:{zone_name:"af-south-1c"}) {
    ok
  }
  updatezones2: updatezones(ids:[2], input:{zone_name:"af-south-1b"}) {
    ok
  }
}

"""
