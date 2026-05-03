<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <!-- define meta settings -->
  <xsl:output method="xml" omit-xml-declaration="no" indent="no"/>

  <!-- select root folder -->
  <xsl:template match="/timetable">
    <timetable station="{@station}">
      <xsl:apply-templates select="s"/>
    </timetable>
  </xsl:template>

  <!-- consider every s-entry -->
  <xsl:template match="s">
    <s id="{@id}" >
      <xsl:apply-templates/>
    </s>
  </xsl:template>

  <!-- consider every ar-entry -->
  <xsl:template match="ar">
    <ar clt="{@clt}" ct="{@ct}" pt="{@pt}" />
  </xsl:template>

    <!-- consider every dp-entry -->
  <xsl:template match="dp">
    <dp clt="{@clt}" ct="{@ct}" pt="{@pt}" />
  </xsl:template>

  <!-- consider every conn-entry -->
  <xsl:template match="conn">
    <ar ts="{@ts}" />
  </xsl:template>

  <!-- consider every hd-entry -->
  <xsl:template match="hd">
    <ar ar="{@ar}" dp="{@dp}" />
  </xsl:template>

</xsl:stylesheet>
