import configparser
import logging
import json
import datetime
import pandas as pd
import requests
import jwt
from collections import OrderedDict

class AdobeAnalytics2:
    """
    Connector for Adobe Analytics 2.0 API using JSON Web Token authentication
    For JWT setup, reference this guide: https://www.adobe.io/apis/experiencecloud/analytics/docs.html#!AdobeDocs/analytics-2.0-apis/master/jwt.md
    For information on how to set up a config.ini file: https://github.com/AdobeDocs/analytics-2.0-apis/tree/master/examples/jwt/python
    """
    # each new instance of AdobeAnalytics is initialized with __init__ which sets the initial state of the object
    def __init__(self):
        config_parser = configparser.ConfigParser()
        config_parser.read("config.ini")
        logging.basicConfig(level="INFO")
        self.__logger = logging.getLogger()
        self.__config = dict(config_parser["default"])
        self.__jwt_token = self._get_jwt_token()
        self.__access_token = self._get_access_token(self.__jwt_token)
        self.__global_company_id = self._get_global_company_id(self.__access_token)
        self.__rsid = "<YOUR RSID>"
        self.__base_url = "{}/{}".format(self.__config["analyticsapiurl"], self.__global_company_id)
        self.__headers = {
            "Authorization": "Bearer {}".format(self.__access_token),
            "x-api-key": self.__config["apikey"],
            "x-proxy-global-company-id": self.__global_company_id
        }

    def _get_jwt_token(self):
        """
        Retrieve the JSON Web Token from Adobe Analytics API 2.0
        For service-to-service integrations, you will also need a JSON Web Token (JWT) that encapsulates your client credentials and authenticates the identity of your integration.
        You then exchange the JWT for the access token that authorizes access.
        """
        with open(self.__config["key_path"], 'r') as file:
            private_key = file.read()

        return jwt.encode({
            "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=300),
            "iss": self.__config["orgid"],
            "sub": self.__config["technicalaccountid"],
            "https://{}/s/ent_reactor_sdk".format(self.__config["imshost"]): True,
            "https://{}/s/ent_analytics_bulk_ingest_sdk".format(self.__config["imshost"]): True,
            "aud": "https://{}/c/{}".format(self.__config["imshost"], self.__config["apikey"])
        }, private_key, algorithm='RS256')
    
    def _get_access_token(self, jwt_token):
        """
        Takes in a JWT in return for an access token that authorizes access to the API.
        """
        post_body = {
            "client_id": self.__config["apikey"],
            "client_secret": self.__config["secret"],
            "jwt_token": jwt_token
        }
        
        self.__logger.info("Sending 'POST' request to {}".format(self.__config["imsexchange"]))
        self.__logger.info("Post body: {}".format(post_body))

        response = requests.post(self.__config["imsexchange"], data=post_body)
        return response.json()["access_token"]
    
    def _get_global_company_id(self, access_token):
        response = requests.get(
            self.__config["discoveryurl"],
            headers={
                "Authorization": "Bearer {}".format(access_token),
                "x-api-key": self.__config["apikey"]
            }
        )

        # Return the first global company id
        return response.json().get("imsOrgs")[0].get("companies")[0].get("globalCompanyId")
    
    def get_report_suites(self, limit=10):
        """
        Retrieve a list of report suites and meta data about each one.
        """
        self.__logger.info(f'{self.__base_url}/collections/suites?limit={limit}')
        response = requests.get(
            f'{self.__base_url}/collections/suites?limit={limit}',
            headers=self.__headers
        )
        return response.json()["content"]
    
    def get_dimensions(self, search_terms = [], exact=False):
        """
        Returns all dimensions in report suite.
        Arguments:
            search_terms (optional): specifies how to return dimensions. If empty, method returns all available dimensions.
            exact: match full word (case-insensitive).
        """
        response = requests.get(
            f'{self.__base_url}/dimensions/?rsid={self.__rsid}',
            headers=self.__headers
        )
        
        res_df = pd.DataFrame(response.json())
        if len(search_terms) == 0:
            df = res_df
        else:
            if exact:
                search_dims = [x.lower() for x in search_terms]
                df = res_df[res_df['name'].str.lower().isin(search_dims)][["id", "name"]]
            else:
                search_dims = "|".join(search_terms)
                df = res_df[res_df['name'].str.contains(search_dims, case=False)][["id", "name"]]
        
        return df
        
    def get_metrics(self, search_terms = [], exact=False):
        """
        Returns all metrics in report suite
        Arguments:
            search_terms (optional): return metrics that match the provided search terms. If not provided, method returns all metrics.
            exact: match full word (case-insensitive).
        """
        response = requests.get(
            f'{self.__base_url}/metrics/?rsid={self.__rsid}',
            headers=self.__headers
        )
        
        res_df = pd.DataFrame(response.json())
        if len(search_terms) == 0:
            df = res_df
        else:
            if exact:
                search_dims = [x.lower() for x in search_terms]
                df = res_df[res_df['name'].str.lower().isin(search_dims)][["id", "name"]]
            else:
                search_dims = "|".join(search_terms)
                df = res_df[res_df['name'].str.contains(search_dims, case=False)][["id", "name"]]
        
        return df

    def get_calculated_metrics(self, search_term = None, limit=100):
        """
        Returns all calculated metrics in report suite
        Arguments:
            search_terms (optional): return metrics that match the provided search terms. If not provided, method returns all.
            exact: match full word (case-insensitive).
        """
        
        assert isinstance(search_term, str), "Search term must be a string."
        
        get_calculated_metrics_url = f'{self.__base_url}/calculatedmetrics/?rsid={self.__rsid}&includeType=all&limit={limit}'
        if search_term:
            get_calculated_metrics_url += f'&name={search_term}'
        
        self.__logger.info(get_calculated_metrics_url)
        response = requests.get(
            get_calculated_metrics_url,
            headers=self.__headers
        )

        res_df = pd.DataFrame(response.json()["content"])
    
        return res_df

    def get_segments(self, search_term=None, limit=10):
        """
        Returns all segments in report suite
        Arguments:
            search_term (optional): return segments that match the provided search term. If not provided, method returns all segments.
        """
        
        assert isinstance(search_term, str), "Search term must be a string."
        
        get_segments_url = f'{self.__base_url}/segments?rsids={self.__rsid}&includeType=all&limit={limit}'
        if search_term:
            get_segments_url += f'&name={search_term}'
        
#         self.__logger.info(get_segments_url)
        response = requests.get(
            get_segments_url,
            headers=self.__headers
        )
        
        res_df = pd.DataFrame(response.json()["content"])
        
        return res_df
    
    def get_metric_names_from_id(self, metric_ids = []):
        """
        Returns user-friendly metric names for a list of metric IDs provided
        Arguments:
            metric_ids: list of metric IDs to find the corresponding metric names for.
        """
        if len(metric_ids) == 0:
            raise Exception("Search terms must be provided.")
        
        response = requests.get(
            f'{self.__base_url}/metrics/?rsid={self.__rsid}',
            headers=self.__headers
        )
        
        res_df = pd.DataFrame(response.json())[["id","name"]]
        df = res_df[res_df['id'].isin(metric_ids)]
        
        return df
    
    def _get_report(self, mets=[], dim=None, segments=None, search_query=None, breakdown_dimension=None, item_id=None, start_date=None, end_date=None, limit=50):
        """
        Returns data pull for the specified metrics and dimension between the specified dates
        
        Arguments:
            met (list): list of metrics for report
            dim (str): specify report row dimension.
            breakdown_dimension (str): specify the dimension used for metric breakdowns
            item_id (num): specify the item_id used for metric breakdowns.
            start_date (str): start date in YYYY-MM-DD format. Defaults to yesterday.
            end_date (str): end date in YYYY-MM-DD format. Defaults to today.
            limit (optional): limit the number of rows returned. Defaults to 50 rows.
        """
        today = datetime.date.today()
        
        if start_date is None:
            start_date = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = today.strftime("%Y-%m-%d")
            

            
        midnight = 'T00:00:00.000'
        date_range = start_date + midnight + '/' + end_date + midnight
        
        def build_report_body_json():
            
            # build metrics
            metrics = []
            metric_filters = []
            for idx, met in enumerate(mets):
                metric = OrderedDict({
                    "columnId": idx,
                    "id": met
                })
                if breakdown_dimension:
                    metric["filters"] = [idx]
                metrics.append(metric)
                
                if dim:
                    metric_filter = OrderedDict({
                        "id": idx,
                        "type": "breakdown",
                        "dimension": breakdown_dimension,
                        "itemId": item_id
                    })
                    
                    metric_filters.append(metric_filter)

                
            # build report body
            report_body = OrderedDict({
                "rsid":self.__rsid,
                "globalFilters":[
                    {
                        "type":"dateRange",
                        "dateRange":date_range
                    }
                ],
                "metricContainer":{
                    "metrics": metrics,
                    "metricFilters":metric_filters
                },
                "dimension":dim,
                "settings":{
                    "countRepeatInstances":True,
                    "limit": limit
                }
                
            })
            
            if segments:
                for segment in segments:
                    report_body["globalFilters"].append(
                        OrderedDict({
                            "type": "segment",
                            "segmentId": segment
                        }))
            
            if dim in search_query.keys():
                report_body["search"] = OrderedDict({
                    "clause": "{}".format(search_query.get(dim))
                })
            
            return report_body

        report_body = build_report_body_json()
#         self.__logger.info(json.dumps(report_body))
        self.__logger.info("Sending POST request to API..")
        response = requests.post(
            url=f'{self.__base_url}/reports/?rsid={self.__rsid}',
            headers=self.__headers,
            json=report_body
        )
        
        return response.json()    
    
    def get_freeform_report(self, mets=[], dims=[], segments=[], search_query=None, start_date=None, end_date=None, limit=50):
        """
        Returns freeform table for the specified metrics and dimensions between the specified dates
        Resource: https://www.adobe.io/apis/experiencecloud/analytics/docs.html#!AdobeDocs/analytics-2.0-apis/master/reporting-multiple-breakdowns.md
        
        Arguments:
            met (list): list of metrics for report
            dim (str): specify report row dimension. If not provided the data pull will just return an aggregated table.
            item_id (num): specify the item_id used for report breakdowns.
            start_date (str): start date in YYYY-MM-DD format. Defaults to yesterday.
            end_date (str): end date in YYYY-MM-DD format. Defaults to today.
            limit (optional): limit the number of rows returned. Defaults to 50 rows.
        """
        
        if len(dims) == 0:
            raise Exception("Please provide at least one dimension.")

        if len(mets) == 0:
            raise Exception("Please provide at least one metric.")
        
        if len(dims) > 2:
            raise Exception("More than 2 dimensions is not currently supported.")
            
        if len(segments) > 1:
            raise Exception("More than 1 segment is not currently supported.")
            

        
        # -------------- api call 1
        for i, dim in enumerate(dims):
            if i == 0:
                result_df = pd.DataFrame()
                resp_1 = self._get_report(mets=mets,
                                          dim=dim,
                                          search_query=search_query,
                                          segments=segments,
                                          start_date=start_date,
                                          end_date=end_date,
                                          limit=limit)
                breakdown_dimension = resp_1["columns"]["dimension"]["id"]
                item_ids = [row["itemId"] for row in resp_1["rows"]]
                
                for j, row in enumerate(resp_1["rows"]):
                    result_df.loc[j, "item_id"] = row["itemId"]
                    result_df.loc[j, dim] = row["value"]
            

        # -------------- api call 2
            if i == 1:
                data_df = pd.DataFrame()
                
                # loop through each item id from api call 1
                for item_id in item_ids:
                    resp_2 = self._get_report(mets=mets,
                                              dim=dim,
                                              segments=segments,
                                              search_query=search_query,
                                              breakdown_dimension=breakdown_dimension,
                                              item_id=item_id,
                                              start_date=start_date,
                                              end_date=end_date)
                    if len(resp_2["rows"]) == 0:
                        raise Exception("API returned no results. If there is a segment filter try removing it.")
                    # for each api call 2, track the item_id, and variable_item_id
                    for row in resp_2["rows"]:
                        data_dict = {}
                        data_dict["item_id"] = [item_id]
                        data_dict[dim] = [row["value"]]
                        
                        # data values are stored in a list of length = len(mets)
                        for j, value in enumerate(row["data"]):
                            data_dict[mets[j]] = [value]
                        
                        data_df = data_df.append(pd.DataFrame.from_dict(data_dict))

                result_df = result_df.merge(data_df, left_on="item_id", right_on="item_id")
            
        return result_df.drop(["item_id"], axis=1)
