from leovendo.exporters import GoogleSheetItemExporter


class GoogleSheetExportPipeline:
    def open_spider(self, spider):
        spreadsheet = spider.settings.get('SPREADSHEET_NAME')
        worksheet = spider.settings.get('WORKSHEET_NAME')
        self.exporter = GoogleSheetItemExporter(spreadsheet, worksheet)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item